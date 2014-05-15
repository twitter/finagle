package com.twitter.finagle.kestrel.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.io.Charsets
import com.twitter.util.Await
import org.specs.SpecificationWithJUnit
import com.twitter.finagle.thrift.{ClientId, ThriftClientFramedCodec}

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

      Await.result(client.delete("foo"))

      "set & get" in {
        Await.result(client.get("foo")) mustEqual None
        Await.result(client.set("foo", "bar"))
        Await.result(client.get("foo")) map { _.toString(Charsets.Utf8) } mustEqual Some("bar")
      }
    }
  }

  "ThriftConnectedClient" should {
    skip("This test requires a Kestrel server to run. Please run manually")

    "simple client" in {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:2229")
        .codec(ThriftClientFramedCodec(Some(ClientId("testcase"))))
        .hostConnectionLimit(1)
        .buildFactory()
      val client = Client.makeThrift(serviceFactory)

      Await.result(client.delete("foo"))

      "set & get" in {
        Await.result(client.get("foo")) mustEqual None
        Await.result(client.set("foo", "bar"))
        Await.result(client.get("foo")) map { _.toString(Charsets.Utf8) } mustEqual Some("bar")
      }
    }
  }
}
