package com.twitter.finagle.kestrel.integration

import org.specs.Specification
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.Client
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

object ClientSpec extends Specification {
  "ConnectedClient" should {
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
    }
  }
}