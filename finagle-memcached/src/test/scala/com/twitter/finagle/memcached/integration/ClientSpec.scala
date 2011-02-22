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

      "gets" in {
        client.set("foo", "bar")()
        client.set("baz", "boing")()
        val result = client.get(Seq("foo", "baz", "notthere"))()
          .map { case (key, value) => (key, value.toString(CharsetUtil.UTF_8)) }
        result mustEqual Map(
          "foo" -> "bar",
          "baz" -> "boing"
        )
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
        client.incr("foo")()    mustEqual Some(1)
        client.incr("foo", 2)() mustEqual Some(3)
        client.decr("foo")()    mustEqual Some(2)
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
