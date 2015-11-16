package com.twitter.finagle.memcached.integration

import _root_.java.lang.{Boolean => JBoolean}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Await

@RunWith(classOf[JUnitRunner])
class SimpleClientTest extends FunSuite with BeforeAndAfter {
  /**
    * Note: This integration test requires a real Memcached server to run.
    */
  var client: Client = null
  var testServer: Option[TestMemcachedServer] = None

  val stats = new SummarizingStatsReceiver

  before {
    testServer = TestMemcachedServer.start()
    if (testServer.isDefined) {
      val service = ClientBuilder()
        .hosts(Seq(testServer.get.address))
        .codec(new Memcached(stats))
        .hostConnectionLimit(1)
        .build()
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
    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    assert(Await.result(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("get") {
    Await.result(client.set("foo", Buf.Utf8("bar")))
    Await.result(client.set("baz", Buf.Utf8("boing")))
    val result = Await.result(client.get(Seq("foo", "baz", "notthere")))
      .map { case (key, Buf.Utf8(value)) => (key, value) }
    assert(result == Map(
      "foo" -> "bar",
      "baz" -> "boing"
    ))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      Await.result(client.set("foos", Buf.Utf8("xyz")))
      Await.result(client.set("bazs", Buf.Utf8("xyz")))
      Await.result(client.set("bazs", Buf.Utf8("zyx")))
      val result = Await.result(client.gets(Seq("foos", "bazs", "somethingelse")))
        .map { case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
          (key, (value, casUnique))
      }

      assert(result == Map(
        "foos" -> ("xyz", "1"),  // the "cas unique" values are predictable from a fresh memcached
        "bazs" -> ("zyx", "3")
      ))
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
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "barrab")
    Await.result(client.prepend("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res2) = Await.result(client.get("foo")).get
    assert(res2 == "rabbarrab")
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
    intercept[ClientError] { Await.result(client.get(nullString)) }
    intercept[ClientError] { Await.result(client.set(nullString, Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("    ", Buf.Utf8("bar"))) }

    try { Await.result(client.set("\t", Buf.Utf8("bar"))) }
    catch { case _: ClientError => fail("\t is allowed") }

    intercept[ClientError] { Await.result(client.set("\r", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\n", Buf.Utf8("bar"))) }
    intercept[ClientError] { Await.result(client.set("\0", Buf.Utf8("bar"))) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { Await.result(client.get(veryLongKey)) }
    assert(Await.ready(client.set(veryLongKey, Buf.Utf8("bar"))).poll.get.isThrow)

    // test other keyed command validation
    val nullSeq:Seq[String] = null
   intercept[ClientError] { Await.result(client.get(nullSeq)) }
   intercept[ClientError] { Await.result(client.gets(nullSeq)) }
   intercept[ClientError] { Await.result(client.gets(Seq(null))) }
   intercept[ClientError] { Await.result(client.gets(Seq(""))) }
   intercept[ClientError] { Await.result(client.gets(Seq("foos", "bad key", "somethingelse"))) }
   intercept[ClientError] { Await.result(client.append("bad key", Buf.Utf8("rab"))) }
   intercept[ClientError] { Await.result(client.prepend("bad key", Buf.Utf8("rab"))) }
   intercept[ClientError] { Await.result(client.replace("bad key", Buf.Utf8("bar"))) }
   intercept[ClientError] { Await.result(client.add("bad key", Buf.Utf8("2"))) }
   intercept[ClientError] { Await.result(client.cas("bad key", Buf.Utf8("z"), Buf.Utf8("2"))) }
   intercept[ClientError] { Await.result(client.incr("bad key")) }
   intercept[ClientError] { Await.result(client.decr("bad key")) }
   intercept[ClientError] { Await.result(client.delete("bad key")) }
  }
}

