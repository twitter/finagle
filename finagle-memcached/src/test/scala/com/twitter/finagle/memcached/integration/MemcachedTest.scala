package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Name
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.Memcached
import com.twitter.finagle.memcached.{Client, PartitionedClient}
import com.twitter.finagle.param
import com.twitter.finagle.Service
import com.twitter.finagle.service.FailureAccrualFactory
import com.twitter.finagle.ShardNotAvailableException
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, MockTimer, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MemcachedTest extends FunSuite with BeforeAndAfter {
  var server1: Option[TestMemcachedServer] = None
  var server2: Option[TestMemcachedServer] = None
  var client: Client = null

  val TimeOut = 15.seconds

  before {
    server1 = TestMemcachedServer.start()
    server2 = TestMemcachedServer.start()
    if (server1.isDefined && server2.isDefined) {
      val n = Name.bound(server1.get.address, server2.get.address)
      client = Memcached.client.newRichClient(n, "test_client")
    }
  }

  after {
    server1.foreach(_.stop())
    server2.foreach(_.stop())
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (server1.isDefined && server2.isDefined) test() else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  test("set & get") {
    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("get") {
    Await.result(client.set("foo", Buf.Utf8("bar")))
    Await.result(client.set("baz", Buf.Utf8("boing")))
    val result =
      Await.result(
        client.get(Seq("foo", "baz", "notthere"))
      ).map { case (key, Buf.Utf8(value)) =>
        (key, value)
      }
    assert(result == Map("foo" -> "bar", "baz" -> "boing"))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      Await.result(client.set("foos", Buf.Utf8("xyz")))
      Await.result(client.set("bazs", Buf.Utf8("xyz")))
      Await.result(client.set("bazs", Buf.Utf8("zyx")))
      val result =
        Await.result(
          client.gets(Seq("foos", "bazs", "somethingelse"))
        ).map { case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
          (key, (value, casUnique))
        }
      val expected =
        Map(
          "foos" ->("xyz", "1"), // the "cas unique" values are predictable from a fresh memcached
          "bazs" ->("zyx", "3")
        )
      assert(result == expected)
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("cas") {
      Await.result(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = Await.result(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("1"))

      assert(!Await.result(client.cas("x", Buf.Utf8("z"), Buf.Utf8("2"))))
      assert(Await.result(client.cas("x", Buf.Utf8("z"), casUnique)))
      val res = Await.result(client.get("x"))
      assert(res.isDefined)
      assert(res.get == Buf.Utf8("z"))
    }
  }

  test("append & prepend") {
    Await.result(client.set("foo", Buf.Utf8("bar")))
    Await.result(client.append("foo", Buf.Utf8("rab")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("barrab"))
    Await.result(client.prepend("foo", Buf.Utf8("rab")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("rabbarrab"))
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    Await.result(client.set("foo", Buf.Utf8("0")))
    assert(Await.result(client.incr("foo"))    == Some(1L))
    assert(Await.result(client.incr("foo", 2)) == Some(3L))
    assert(Await.result(client.decr("foo"))    == Some(2L))

    Await.result(client.set("foo", Buf.Utf8("0")))
    assert(Await.result(client.incr("foo"))    == Some(1L))
    val l = 1L << 50
    assert(Await.result(client.incr("foo", l)) == Some(l + 1L))
    assert(Await.result(client.decr("foo"))    == Some(l))
    assert(Await.result(client.decr("foo", l)) == Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      val stats = Await.result(client.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("STAT"))
      }
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { Await.result(client.get("fo o")) }
    intercept[ClientError] { Await.result(client.set("", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    intercept[ClientError] { Await.result(client.get("foo   ")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    val nullString: String = null
    intercept[NullPointerException] { Await.result(client.get(nullString)) }
    intercept[NullPointerException] { Await.result(client.set(nullString, Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("    ", Buf.Utf8("bar"))) }

    assert(Await.result(client.set("\t", Buf.Utf8("bar"))) == ()) // "\t" is a valid key
    intercept[ClientError] { Await.result(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\0", Buf.Utf8("bar"))) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { Await.result(client.get(veryLongKey)) }
    intercept[ClientError] { Await.result(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    val nullSeq:Seq[String] = null
    intercept[NullPointerException] { Await.result(client.get(nullSeq)) }
    intercept[ClientError] { Await.result(client.append("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { Await.result(client.prepend("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { Await.result(client.replace("bad key", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.add("bad key", Buf.Utf8("2"))) }
    intercept[ClientError] { Await.result(client.cas("bad key", Buf.Utf8("z"), Buf.Utf8("2"))) }
    intercept[ClientError] { Await.result(client.incr("bad key")) }
    intercept[ClientError] { Await.result(client.decr("bad key")) }
    intercept[ClientError] { Await.result(client.delete("bad key")) }
  }

  test("re-hash when a bad host is ejected") {
    val n = Name.bound(server1.get.address, server2.get.address)
    client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(Memcached.param.EjectFailedHost(true))
      .newRichClient(n, "test_client")
    val partitionedClient = client.asInstanceOf[PartitionedClient]

    // set values
    Await.result(Future.collect(
      (0 to 20).map { i =>
        client.set(s"foo$i", Buf.Utf8(s"bar$i"))
      }
    ), TimeOut)

    // shutdown one memcache host
    server2.foreach(_.stop())

    // trigger ejection
    for (i <- 0 to 20) {
      Await.ready(client.get(s"foo$i"), TimeOut)
    }

    // one memcache host alive
    val clientSet =
      (0 to 20).foldLeft(Set[Client]()){ case (s, i) =>
        val c = partitionedClient.clientOf(s"foo$i")
        s + c
      }
    assert(clientSet.size == 1)

    // previously set values have cache misses
    var cacheMisses = 0
    for (i <- 0 to 20) {
      if (Await.result(client.get(s"foo$i"), TimeOut) == None) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)
  }

  test("host comes back into ring after being ejected") {
    import com.twitter.finagle.memcached.protocol._

    class MockedMemcacheServer extends Service[Command, Response] {
      def apply(command: Command) = command match {
        case Get(key) => Future.value(Values(List(Value(Buf.Utf8("foo"), Buf.Utf8("bar")))))
        case Set(_, _, _, _) => Future.value(Error(new Exception))
      }
    }

    val cacheServer = Memcached.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new MockedMemcacheServer)

    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver

    val client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(Memcached.param.EjectFailedHost(true))
      .configured(param.Timer(timer))
      .configured(param.Stats(statsReceiver))
      .newRichClient(Name.bound(cacheServer.boundAddress), "cacheClient")

    Time.withCurrentTimeFrozen { timeControl =>

      // Send a bad request
      intercept[Exception] { Await.result(client.set("foo", Buf.Utf8("bar"))) }

      // Node should have been ejected
      assert(statsReceiver.counters.get(List("ejections")) == Some(1))

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        Await.result(client.get(s"foo"))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go through
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(Await.result(client.get(s"foo")).get == Buf.Utf8("bar"))
    }
  }
}
