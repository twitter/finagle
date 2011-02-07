package com.twitter.finagle.kestrel.integration

import org.specs.Specification
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.Client
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import collection.mutable.ListBuffer
import com.twitter.util.CountDownLatch
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffer


object ClientSpec extends Specification {
  "ConnectedClient" should {
    skip("This test requires a Kestrel server to run. Please run manually")

    "simple client" in {
      val service = ClientBuilder()
        .hosts("localhost:22133")
        .codec(new Kestrel)
        .build()
      val client = Client(service)

      client.delete("foo")()

      "set & get" in {
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "receive" in {
        "no errors" in {
          val result = new ListBuffer[String]
          client.set("foo", "bar")()
          client.set("foo", "baz")()
          client.set("foo", "boing")()

          val channel = client.sink("foo")
          val latch = new CountDownLatch(3)
          channel.respond(this) {
            case item =>
              result += item.toString(CharsetUtil.UTF_8)
              latch.countDown()
          }
          latch.await(1.second)
          channel.close ()
          result mustEqual List("bar", "baz", "boing")
        }

        "transactionality in the presence of errors" in {
          client.set("foo", "bar")()

          var result: ChannelBuffer = null
          var channel = client.sink("foo")
          val latch = new CountDownLatch(1)
          channel.respond(this) { item =>
            throw new Exception
          }
          channel = client.sink("foo")
          channel.respond(this) {
            case item =>
              result = item
              latch.countDown()
          }
          latch.await(1.second)
          channel.close()
          result.toString(CharsetUtil.UTF_8) mustEqual "bar"
        }
      }
    }
  }
}