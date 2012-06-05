package com.twitter.finagle.kestrel.integration

import org.specs.SpecificationWithJUnit
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

class ClientSpec extends SpecificationWithJUnit {
  "ConnectedClient" should {
    skip("This test requires a Kestrel server to run. Please run manually")

    "simple client" in {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:22133")
        .codec(Kestrel())
        .hostConnectionLimit(1)
        .buildFactory()
      val client = Client(serviceFactory)

      client.delete("foo")()

      "set & get" in {
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")() map { _.toString(CharsetUtil.UTF_8) } mustEqual Some("bar")
      }
    }
  }
}
