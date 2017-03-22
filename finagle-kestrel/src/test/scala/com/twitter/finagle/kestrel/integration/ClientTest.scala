package com.twitter.finagle.kestrel.integration

import com.twitter.conversions.time._
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.Kestrel
import com.twitter.finagle.thrift.ClientId
import com.twitter.io.Buf
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {

  // Requires external Kestrel server to run
  if (Option(System.getProperty("USE_EXTERNAL_KESTREL")).isDefined) {
    test("Connected client should set & get") {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:22133")
        .stack(Kestrel.client)
        .hostConnectionLimit(1)
        .buildFactory()
      val client = Client(serviceFactory)

      Await.result(client.delete("foo"), 2.seconds)

      assert(Await.result(client.get("foo"), 2.seconds) == None)
      Await.result(client.set("foo", Buf.Utf8("bar")), 2.seconds)
      val rep = Await.result(client.get("foo"), 2.seconds).map {
        case Buf.Utf8(s) => s
      }
      assert(rep == Some("bar"))
    }

    test("ThriftConnectedClient should set & get") {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:2229")
        .stack(ThriftMux.client.withClientId(ClientId("testcase")))
        .buildFactory()
      val client = Client.makeThrift(serviceFactory)

      Await.result(client.delete("foo"), 2.seconds)

      assert(Await.result(client.get("foo"), 2.seconds) == None)
      Await.result(client.set("foo", Buf.Utf8("bar")), 2.seconds)
      val rep = Await.result(client.get("foo"), 2.seconds).map {
        case Buf.Utf8(s) => s
      }
      assert(rep == Some("bar"))
    }

    test("Connected stack client should set & get") {
      val serviceFactory = ClientBuilder()
        .hosts("localhost:22133")
        .stack(Kestrel.client)
        .buildFactory()
      val client = Client(serviceFactory)

      Await.result(client.delete("foo"), 2.seconds)

      assert(Await.result(client.get("foo"), 2.seconds) == None)
      Await.result(client.set("foo", Buf.Utf8("bar")), 2.seconds)
      val rep = Await.result(client.get("foo"), 2.seconds).map {
        case Buf.Utf8(s) => s
      }
      assert(rep == Some("bar"))
    }
  }
}

