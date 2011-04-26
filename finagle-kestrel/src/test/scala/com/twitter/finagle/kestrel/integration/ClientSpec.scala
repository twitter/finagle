package com.twitter.finagle.kestrel.integration

import org.specs.Specification
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.Client
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import collection.mutable.ListBuffer
import com.twitter.util.CountDownLatch
import com.twitter.util.Future
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffer

object ClientSpec extends Specification {
  "ConnectedClient" should {
    skip("This test requires a Kestrel server to run. Please run manually")

    "simple client" in {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:22133")
        .codec(new Kestrel)
        .hostConnectionLimit(1)
        .buildFactory()
      val client = Client(serviceFactory)

      client.delete("foo")()

      "set & get" in {
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "from" in {
        "no errors" in {
          val result = new ListBuffer[String]
          client.set("foo", "bar")()
          client.set("foo", "baz")()
          client.set("foo", "boing")()

          val channel = client.from("foo")
          val latch = new CountDownLatch(3)
          val o = channel.respond { item =>
            Future {
              result += item.toString(CharsetUtil.UTF_8)
              latch.countDown()
            }
          }
          latch.await(1.second)
          o.dispose()
          result mustEqual List("bar", "baz", "boing")
        }

        "transactionality in the presence of errors" in {
          client.set("foo", "bar")()

          var result: ChannelBuffer = null
          var channel = client.from("foo")
          val latch = new CountDownLatch(1)
          val o1 = channel.respond { item =>
            throw new Exception
          }
          channel = client.from("foo")
          val o2 = channel.respond { item =>
            Future {
              result = item
              latch.countDown()
            }
          }
          latch.within(1.second)
          o1.dispose(); o2.dispose()
          result.toString(CharsetUtil.UTF_8) mustEqual "bar"
        }
      }

      "to" in {
        val channelSource = client.to("foo")
        channelSource.send("bar")
        channelSource.send("baz")
        channelSource.send("boing")
        channelSource.close()

        client.get("foo", 2.second)().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
        client.get("foo", 1.second)().get.toString(CharsetUtil.UTF_8) mustEqual "baz"
        client.get("foo", 1.second)().get.toString(CharsetUtil.UTF_8) mustEqual "boing"
        client.get("foo", 1.second)() mustEqual None
      }
    }
  }
}
