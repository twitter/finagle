package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ClusterClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Await, Awaitable, Future, Time}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ClusterClientIntegrationSuite extends ClusterClientTest {

  val primaryCount: Int = 3

  test("Correctly retrieve CLUSTER INFO", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val info = Await.result(client.clusterInfo)

      // cluster info has 11 entries
      assert(info.size == 11)
      assert(info.contains("cluster_size"))
    }
  }

  test("Correctly retrieve empty slots using SLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val slots = Await.result(client.slots)

      assert(slots == Seq())
    }
  }

  test("Correctly assign slots to a server using ADDSLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      Await.result(client.addSlots((0 until 100)))

      val info = Await.result(client.clusterInfo)

      assert(info.get("cluster_slots_assigned") == Some("100"))
    }
  }

  test("Correctly retrieve assigned slots using SLOTS", RedisTest, ClientTest) {
    withClusterClient(0) { client =>
      val slots = Await.result(client.slots)

      assert(slots.size == 1)
      val s = slots.head
      assert(s.start == 0 && s.end == 99 && s.master.host == "" && s.replicas.size == 0)
    }
  }

}
