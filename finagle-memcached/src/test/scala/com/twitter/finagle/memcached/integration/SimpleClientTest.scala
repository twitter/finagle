package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Memcached.UsePushMemcachedToggleName
import com.twitter.finagle.{Address, Memcached, Name}
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.toggle.flag
import com.twitter.io.Buf
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import com.twitter.util.{Await, Awaitable}
import java.net.InetSocketAddress
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

abstract class SimpleClientTest extends FunSuite with BeforeAndAfter {

  /**
   * Note: This integration test requires a real Memcached server to run.
   */
  var client: Client = null
  var testServer: Option[TestMemcachedServer] = None

  val stats = new SummarizingStatsReceiver

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

  protected def baseClient: Memcached.Client

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined) {
      val address = Address(testServer.get.address.asInstanceOf[InetSocketAddress])
      val service = baseClient
        .withStatsReceiver(stats)
        .connectionsPerEndpoint(1)
        .newService(Name.bound(address), "memcache")
      client = Client(service)
    }
  }

  after {
    if (testServer.isDefined)
      testServer map { _.stop() }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (testServer.isDefined) test()
    else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  test("set & get") {
    awaitResult(client.delete("foo"))
    assert(awaitResult(client.get("foo")) == None)
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("get") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.get(Seq("foo", "baz", "notthere")))
      .map { case (key, Buf.Utf8(value)) => (key, value) }
    assert(
      result == Map(
        "foo" -> "bar",
        "baz" -> "boing"
      )
    )
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      awaitResult(client.set("foos", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("zyx")))
      val result = awaitResult(client.gets(Seq("foos", "bazs", "somethingelse")))
        .map {
          case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
            (key, (value, casUnique))
        }

      assert(
        result == Map(
          "foos" -> (("xyz", "1")), // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> (("zyx", "3"))
        )
      )
    }
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("cas") {
      awaitResult(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = awaitResult(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("1"))

      assert(!awaitResult(client.checkAndSet("x", Buf.Utf8("z"), Buf.Utf8("2")).map(_.replaced)))
      assert(
        awaitResult(client.checkAndSet("x", Buf.Utf8("z"), casUnique).map(_.replaced)).booleanValue
      )
      val res = awaitResult(client.get("x"))
      assert(res.isDefined)
      assert(res.get == Buf.Utf8("z"))
    }
  }

  test("append & prepend") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.append("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res) = awaitResult(client.get("foo")).get
    assert(res == "barrab")
    awaitResult(client.prepend("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res2) = awaitResult(client.get("foo")).get
    assert(res2 == "rabbarrab")
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    assert(awaitResult(client.incr("foo", 2)) == Some(3L))
    assert(awaitResult(client.decr("foo")) == Some(2L))

    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    val l = 1L << 50
    assert(awaitResult(client.incr("foo", l)) == Some(l + 1L))
    assert(awaitResult(client.decr("foo")) == Some(l))
    assert(awaitResult(client.decr("foo", l)) == Some(0L))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("stats") {
      val stats = awaitResult(client.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat =>
        assert(stat.startsWith("STAT"))
      }
    }
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] { awaitResult(client.get("fo o")) }
    intercept[ClientError] { awaitResult(client.set("", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    intercept[ClientError] { awaitResult(client.get("foo   ")) }
    intercept[ClientError] { awaitResult(client.get("    foo")) }
    intercept[ClientError] { awaitResult(client.get(null: String)) }
    intercept[ClientError] { awaitResult(client.set(null: String, Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("    ", Buf.Utf8("bar"))) }

    try { awaitResult(client.set("\t", Buf.Utf8("bar"))) } catch {
      case _: ClientError => fail("\t is allowed")
    }

    intercept[ClientError] { awaitResult(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.set("\u0000", Buf.Utf8("bar"))) }

    val veryLongKey =
      "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { awaitResult(client.get(veryLongKey)) }
    intercept[ClientError] { awaitResult(client.set(veryLongKey, Buf.Utf8("bar"))) }

    // test other keyed command validation
    intercept[ClientError] { awaitResult(client.get(null: Seq[String])) }
    intercept[ClientError] { awaitResult(client.gets(null: Seq[String])) }
    intercept[ClientError] { awaitResult(client.gets(Seq(null))) }
    intercept[ClientError] { awaitResult(client.gets(Seq(""))) }
    intercept[ClientError] { awaitResult(client.gets(Seq("foos", "bad key", "somethingelse"))) }
    intercept[ClientError] { awaitResult(client.append("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.prepend("bad key", Buf.Utf8("rab"))) }
    intercept[ClientError] { awaitResult(client.replace("bad key", Buf.Utf8("bar"))) }
    intercept[ClientError] { awaitResult(client.add("bad key", Buf.Utf8("2"))) }
    intercept[ClientError] {
      awaitResult(client.checkAndSet("bad key", Buf.Utf8("z"), Buf.Utf8("2")))
    }
    intercept[ClientError] { awaitResult(client.incr("bad key")) }
    intercept[ClientError] { awaitResult(client.decr("bad key")) }
    intercept[ClientError] { awaitResult(client.delete("bad key")) }
  }
}

class SimplePushClientTest extends SimpleClientTest {
  protected def baseClient: Memcached.Client =
    flag.overrides.let(UsePushMemcachedToggleName, 1.0) {
      Memcached.client
    }

  test("Push client uses Netty4PushTransporter") {
    val simple = new SimpleRegistry()
    val address = Address(testServer.get.address.asInstanceOf[InetSocketAddress])
    GlobalRegistry.withRegistry(simple) {
      flag.overrides.let(UsePushMemcachedToggleName, 1.0) {
        val client = Memcached.client.newService(Name.bound(address), "memcache")
        client(Quit())
        val entries = simple.toSet
        assert(entries.contains(
          Entry(Seq("client", "memcached", "memcache", "Transporter"), "Netty4PushTransporter")))
      }
    }
  }
}

class SimpleNonPushClientTest extends SimpleClientTest {
  protected def baseClient: Memcached.Client =
    flag.overrides.let(UsePushMemcachedToggleName, 0.0) {
      Memcached.client
    }

  test("Non-push client uses Netty4Transporter") {
    val simple = new SimpleRegistry()
    val address = Address(testServer.get.address.asInstanceOf[InetSocketAddress])
    GlobalRegistry.withRegistry(simple) {
      flag.overrides.let(UsePushMemcachedToggleName, 0.0) {
        val client = Memcached.client.newService(Name.bound(address), "memcache")
        client(Quit())
        val entries = simple.toSet
        assert(entries.contains(
          Entry(Seq("client", "memcached", "memcache", "Transporter"), "Netty4Transporter")))
      }
    }
  }
}
