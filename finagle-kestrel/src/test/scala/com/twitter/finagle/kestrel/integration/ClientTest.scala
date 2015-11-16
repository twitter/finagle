package com.twitter.finagle.kestrel.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.thrift.{ClientId, ThriftClientFramedCodec}
import com.twitter.io.Buf
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  ignore("This test requires a Kestrel server to run. Please run manually. " +
    "Connected client should set & get") {
    val serviceFactory = ClientBuilder()
      .hosts("localhost:22133")
      .codec(Kestrel())
      .hostConnectionLimit(1)
      .buildFactory()
    val client = Client(serviceFactory)

    Await.result(client.delete("foo"))

    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val rep = Await.result(client.get("foo")) map {
      case Buf.Utf8(s) => s
    }
    assert(rep == Some("bar"))
  }

  ignore("This test requires a Kestrel server to run. Please run manually. " +
    "ThriftConnectedClient should set & get") {
    val serviceFactory = ClientBuilder()
      .hosts("localhost:2229")
      .codec(ThriftClientFramedCodec(Some(ClientId("testcase"))))
      .hostConnectionLimit(1)
      .buildFactory()
    val client = Client.makeThrift(serviceFactory)

    Await.result(client.delete("foo"))

    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val rep = Await.result(client.get("foo")) map {
      case Buf.Utf8(s) => s
    }
    assert(rep == Some("bar"))
  }
}
