package com.twitter.finagle.memcached.integration

import org.specs.Specification

import org.jboss.netty.util.CharsetUtil

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.{Server, Client, KetamaClientBuilder}
import com.twitter.util.RandomSocket

object ClientSpec extends Specification {
  "ConnectedClient" should {
    /**
     * Note: This integration test requires a real Memcached server to run.
     */
    var client: Client = null

    doBefore {
      ExternalMemcached.start()
      val service = ClientBuilder()
        .hosts(Seq(ExternalMemcached.address.get))
        .codec(new Memcached)
        .hostConnectionLimit(1)
        .build()
      client = Client(service)
    }

    doAfter {
      ExternalMemcached.stop()
    }

    "simple client" in {
      "set & get" in {
        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "get" in {
        client.set("foo", "bar")()
        client.set("baz", "boing")()
        val result = client.get(Seq("foo", "baz", "notthere"))()
          .map { case (key, value) => (key, value.toString(CharsetUtil.UTF_8)) }
        result mustEqual Map(
          "foo" -> "bar",
          "baz" -> "boing"
        )
      }

      "gets" in {
        client.set("foos", "xyz")()
        client.set("bazs", "xyz")()
        client.set("bazs", "zyx")()
        val result = client.gets(Seq("foos", "bazs", "somethingelse"))()
          .map { case (key, (value, casUnique)) =>
            (key, (value.toString(CharsetUtil.UTF_8), casUnique.toString(CharsetUtil.UTF_8)))
          }

        result mustEqual Map(
          "foos" -> ("xyz", "1"),  // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> ("zyx", "3")
        )
      }

      "cas" in {
        client.set("x", "y")()
        val Some((value, casUnique)) = client.gets("x")()
        value.toString(CharsetUtil.UTF_8) must be_==("y")
        casUnique.toString(CharsetUtil.UTF_8) must be_==("1")

        client.cas("x", "z", "2")() must beFalse
        client.cas("x", "z", casUnique)() must beTrue
        val res = client.get("x")()
        res must beSomething
        res.get.toString(CharsetUtil.UTF_8) must be_==("z")
      }

      "append & prepend" in {
        client.set("foo", "bar")()
        client.append("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "barrab"
        client.prepend("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "rabbarrab"
      }

      "incr & decr" in {
        client.set("foo", "")()
        client.incr("foo")()    mustEqual Some(1L)
        client.incr("foo", 2)() mustEqual Some(3L)
        client.decr("foo")()    mustEqual Some(2L)

        client.set("foo", "")()
        client.incr("foo")()    mustEqual Some(1L)
        val l = 1L << 50
        client.incr("foo", l)() mustEqual Some(l + 1L)
        client.decr("foo")()    mustEqual Some(l)
        client.decr("foo", l)() mustEqual Some(0L)
      }
    }

    "ketama client" in {
      /**
       * We already proved above that we can hit a real memcache server,
       * so we can use our own for the partitioned client test.
       */
      var server1: Server = null
      var server2: Server = null
      val address1 = RandomSocket()
      val address2 = RandomSocket()

      doBefore {
        server1 = new Server(address1)
        server1.start()
        server2 = new Server(address2)
        server2.start()
      }

      doAfter {
        server1.stop()
        server2.stop()
      }

      val client = (new KetamaClientBuilder())
        .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
        .build()

      "doesn't blow up" in {
        client.delete("foo")()
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }
    }
  }
}
