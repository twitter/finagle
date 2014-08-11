package com.twitter.finagle.kestrel.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.io.Charsets
import com.twitter.util.Await
import com.twitter.finagle.thrift.{ClientId, ThriftClientFramedCodec}

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Suites}

@RunWith(classOf[JUnitRunner])
class ClientTest extends Suites (
  new ConnectedClientTest, 
  new ThriftConnectedClientTest
)

class ConnectedClientTest extends FunSuite {
  val serviceFactory = ClientBuilder()
    .hosts("localhost:22133")
    .codec(Kestrel())
    .hostConnectionLimit(1)
    .buildFactory()
  val client = Client(serviceFactory)

  Await.result(client.delete("foo"))

  ignore("simple clientset & get") {
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).map(f => f.toString(Charsets.Utf8)) === Some("bar"))
  }
}

class ThriftConnectedClientTest extends FunSuite {
  val serviceFactory = ClientBuilder()
    .hosts("localhost:2229")
    .codec(ThriftClientFramedCodec(Some(ClientId("testcase"))))
    .hostConnectionLimit(1)
    .buildFactory()
  val client = Client.makeThrift(serviceFactory)

  Await.result(client.delete("foo"))

  ignore("set & get") {
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).map(f => f.toString(Charsets.Utf8)) == Some("bar"))
  }
}
