package com.twitter.finagle.memcached.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Name
import com.twitter.finagle.memcached.param.EjectFailedHost
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.{Client, Memcached, PartitionedClient}
import com.twitter.finagle.service.FailureAccrualFactory
import com.twitter.io.Charsets
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Outcome}

@RunWith(classOf[JUnitRunner])
class MemcachedTest extends FunSuite with BeforeAndAfter {
  var server1: Option[TestMemcachedServer] = None
  var server2: Option[TestMemcachedServer] = None
  var client: Client = null

  before {
    server1 = TestMemcachedServer.start()
    server2 = TestMemcachedServer.start()
    if (server1.isDefined && server2.isDefined) {
      client =
        Memcached("memcache")
          .newClient(Name.bound(server1.get.address, server2.get.address))
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
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) == "bar")
  }

  test("get") {
    Await.result(client.set("foo", "bar"))
    Await.result(client.set("baz", "boing"))
    val result =
      Await.result(
        client.get(Seq("foo", "baz", "notthere"))
      ).map { case (key, value) =>
        (key, value.toString(Charsets.Utf8))
      }
    assert(result == Map("foo" -> "bar", "baz" -> "boing"))
  }

  if (Option(System.getProperty("USE_EXTERNAL_MEMCACHED")).isDefined) {
    test("gets") {
      Await.result(client.set("foos", "xyz"))
      Await.result(client.set("bazs", "xyz"))
      Await.result(client.set("bazs", "zyx"))
      val result =
        Await.result(
          client.gets(Seq("foos", "bazs", "somethingelse"))
        ).map { case (key, (value, casUnique)) =>
          (key, (value.toString(Charsets.Utf8), casUnique.toString(Charsets.Utf8)))
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
      Await.result(client.set("x", "y"))
      val Some((value, casUnique)) = Await.result(client.gets("x"))
      assert(value.toString(Charsets.Utf8) == "y")
      assert(casUnique.toString(Charsets.Utf8) == "1")

      assert(!Await.result(client.cas("x", "z", "2")))
      assert(Await.result(client.cas("x", "z", casUnique)))
      val res = Await.result(client.get("x"))
      assert(res.isDefined)
      assert(res.get.toString(Charsets.Utf8) == "z")
    }
  }


  test("append & prepend") {
    Await.result(client.set("foo", "bar"))
    Await.result(client.append("foo", "rab"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) == "barrab")
    Await.result(client.prepend("foo", "rab"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) == "rabbarrab")
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    Await.result(client.set("foo", "0"))
    assert(Await.result(client.incr("foo"))    == Some(1L))
    assert(Await.result(client.incr("foo", 2)) == Some(3L))
    assert(Await.result(client.decr("foo"))    == Some(2L))

    Await.result(client.set("foo", "0"))
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
    intercept[ClientError] { Await.result(client.set("", "bar")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    intercept[ClientError] { Await.result(client.get("foo   ")) }
    intercept[ClientError] { Await.result(client.get("    foo")) }
    val nullString: String = null
    intercept[NullPointerException] { Await.result(client.get(nullString)) }
    intercept[NullPointerException] { Await.result(client.set(nullString, "bar")) }
    intercept[ClientError] { Await.result(client.set("    ", "bar")) }

    assert(Await.result(client.set("\t", "bar")) == ()) // "\t" is a valid key
    intercept[ClientError] { Await.result(client.set("\r", "bar")) }
    intercept[ClientError] { Await.result(client.set("\n", "bar")) }
    intercept[ClientError] { Await.result(client.set("\0", "bar")) }

    val veryLongKey = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] { Await.result(client.get(veryLongKey)) }
    intercept[ClientError] { Await.result(client.set(veryLongKey, "bar")) }

    // test other keyed command validation
    val nullSeq:Seq[String] = null
    intercept[NullPointerException] { Await.result(client.get(nullSeq)) }
    intercept[ClientError] { Await.result(client.append("bad key", "rab")) }
    intercept[ClientError] { Await.result(client.prepend("bad key", "rab")) }
    intercept[ClientError] { Await.result(client.replace("bad key", "bar")) }
    intercept[ClientError] { Await.result(client.add("bad key", "2")) }
    intercept[ClientError] { Await.result(client.cas("bad key", "z", "2")) }
    intercept[ClientError] { Await.result(client.incr("bad key")) }
    intercept[ClientError] { Await.result(client.decr("bad key")) }
    intercept[ClientError] { Await.result(client.delete("bad key")) }
  }

  test("re-hash when a bad host is ejected") {
    client =
      Memcached("memcache")
        .configured(FailureAccrualFactory.Param(5, () => 10.minutes))
        .configured(EjectFailedHost(true))
        .newClient(Name.bound(server1.get.address, server2.get.address))
    val partitionedClient = client.asInstanceOf[PartitionedClient]

    // set values
    for (i <- 0 to 20) {
      client.set(s"foo$i", s"bar$i")
    }

    val keys = (0 to 20).map(i => s"foo$i")

    // shutdown one memcache host
    server2.foreach(_.stop())

    //trigger ejection
    for (i <- 0 to 20) {
      try {
        Await.result(client.get(s"foo$i"))
      } catch { case _ => () }
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
      if (Await.result(client.get(s"foo$i")) == None) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)
  }
}