package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClusterClient, ClusterClientTest, ServerError,
  ClusterAskError, ClusterMovedError, ClusterTooManyRedirectsError,
  ClusterTryAgainError}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.{BufToString, RedisCluster}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Await, Awaitable, Future, Time}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ClusterClientRedirectionIntegrationSuite extends ClusterClientTest {

  // Generate hash keys/slots for testing:
  // while true; do r=`openssl rand -hex 4`;
  // s=`echo "cluster keyslot $r" | redis-cli | cut -f2`; echo "$s,$r"; done

  val primaryCount: Int = 3

  private val slotAndKeys = Seq(
    (3,"f80d8469"),
    (4,"5985d17b"),
    (5,"7c3942ce"),
    (7,"2fed0338"),
    (8,"355c73ad"),
    (9,"aecd0efe"),
    (12,"d1501208"),
    (13,"7c7a0293"),
    (15,"5c2ddecd"),
    (17,"e97591e5")
  ).toIterator

  // Resharding is depending on a correctly started/configured cluster
  test("Cluster is configured and started correctly", RedisTest, ClientTest) {
    startCluster()
  }

  test("Correctly throw a ClusterMovedError on access to non-owned slot", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("foo")
      val value = Buf.Utf8("bar")

      // test that the responsible server is not throwing an error (A owns all slots)
      Await.result(a.set(key, value))
      assert(Await.result(a.get(key)) == Some(value))

      // retrieve the value from another server (MOVED redirect)
      intercept[ClusterMovedError](Await.result(b.get(key)))
    } 
  }

  test("Correctly throw a ClusterMovedError after resharding", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val (slotId, k) = slotAndKeys.next
      val key = Buf.Utf8(k) // slot 42
      val value = Buf.Utf8("bar")

      // test the server that is responsible
      Await.result(a.set(key, value))
      assert(Await.result(a.getKeysInSlot(slotId)) == Seq(key))
      assert(Await.result(b.getKeysInSlot(slotId)) == Seq())

      assertReshard(a, b, slotId)

      assert(Await.result(b.getKeysInSlot(slotId)) == Seq(key))

      // retrieve the value from another server (MOVED error)
      intercept[ClusterMovedError](Await.result(a.get(key)))
    }
  }

  test("Correctly raise ClusterAskError when a slot is MIGRATING", RedisTest, ClientTest) {
    withClusterClients(0, 2) { case Seq(a, c) =>
      val (slotId, k) = slotAndKeys.next
      val key = Buf.Utf8(k)

      // start migrating slot to another node (C)
      val aId = Await.result(a.nodeId)
      val cId = Await.result(c.nodeId)
      assert(aId.nonEmpty)
      assert(cId.nonEmpty)

      Await.result(c.setSlotImporting(slotId, aId.get))
      Await.result(a.setSlotMigrating(slotId, cId.get))

      // make sure that C returns moved since its not responsible
      intercept[ClusterMovedError](Await.result(c.get(key)))

      // retrieve a key at A that doesnt exist to trigger ask redirection
      intercept[ClusterAskError](Await.result(a.get(key)))

      // send asking to C before the get to see if it has the key
      val f = for { _ <- c.asking(); resp <- c.get(key) } yield resp
      assert(Await.result(f) == None)
    }
  }

  test("Correctly raise ClusterTryAgainError with a multi-key operation during MIGRATING", RedisTest, ClientTest) {
    withClusterClients(0, 2) { case Seq(a, c) =>
      val (slotId, k) = slotAndKeys.next
      val keys = Seq(Buf.Utf8(s"{$k}.foo"), Buf.Utf8(s"{$k}.bar"))

      // start migrating the slot to C
      val aId = Await.result(a.nodeId)
      val cId = Await.result(c.nodeId)
      assert(aId.nonEmpty)
      assert(cId.nonEmpty)

      Await.result(c.setSlotImporting(slotId, aId.get))
      Await.result(a.setSlotMigrating(slotId, cId.get))

      // A will redirect with ASK to C
      intercept[ClusterAskError](Await.result(a.mGet(keys)))

      // make sure that C returns try again since the slot is importing
      // this will only occur if we are asking
      intercept[ClusterTryAgainError](Await.result(for {
        _ <- c.asking()
        _ <- c.mGet(keys) 
      } yield ()))
    }
  }
}

