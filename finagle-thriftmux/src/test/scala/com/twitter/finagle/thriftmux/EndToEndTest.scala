package com.twitter.finagle.thriftmux

import com.twitter.finagle._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.dispatch.{PipeliningDispatcher, SerialClientDispatcher}
import com.twitter.finagle.server.DefaultServer
import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftFramedTransporter, ThriftClientRequest}
import com.twitter.finagle.thriftmux.thriftscala.{TestService, TestService$FinagleService}
import com.twitter.finagle.tracing._
import com.twitter.finagle.tracing.Annotation.{ServerRecv, ClientSend}
import com.twitter.util.{Await, Future, Promise, RandomSocket}
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  trait ThriftMuxTestServer {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })
  }

  test("end-to-end Scrooge3") {
    new ThriftMuxTestServer {
      val client = ThriftMux.newIface[TestService.FutureIface](server)
      assert(Await.result(client.query("ok")) == "okok")
    }
  }

  test("thriftmux server + Finagle thrift client") {
    new ThriftMuxTestServer {
      val client = Thrift.newIface[TestService.FutureIface](server)
      1 to 5 foreach { _ =>
        assert(Await.result(client.query("ok")) == "okok")
      }
    }
  }

  test("ServerBuilder thriftmux server + Finagle thrift client") {
    val address = RandomSocket()
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    }

    val service = new TestService$FinagleService(iface, Thrift.protocolFactory)

    val server = ServerBuilder()
      .stack(exp.ThriftMuxServer)
      .bindTo(address)
      .name("ThriftMuxServer")
      .build(service)

    val client = Thrift.newIface[TestService.FutureIface]("localhost:" + address.getPort)

    1 to 5 foreach { _ =>
      assert(Await.result(client.query("ok")) == "okok")
    }
  }

  test("""|thriftmux server + Finagle thrift client: client should receive a
      | TApplicationException if the server throws an unhandled exception
       """.stripMargin) {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = throw new Exception("sad panda")
    })
    val client = Thrift.newIface[TestService.FutureIface](server)
    val thrown = intercept[Exception] { Await.result(client.query("ok")) }
    assert(thrown.getMessage === "Internal error processing query: 'java.lang.Exception: sad panda'")
  }

  test("thriftmux server + Finagle thrift client: traceId should be passed from client to server") {
    @volatile var cltTraceId: Option[TraceId] = None
    @volatile var srvTraceId: Option[TraceId] = None
    val tracer = new Tracer {
      def record(record: Record) {
        record match {
          case Record(id, _, ServerRecv(), _) => srvTraceId = Some(id)
          case Record(id, _, ClientSend(), _) => cltTraceId = Some(id)
          case _ =>
        }
      }
      def sampleTrace(traceId: TraceId): Option[Boolean] = None
    }

    // TODO: temporary workaround to capture the ServerRecv record.
    object TestThriftMuxer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
      "thrift", ThriftMuxListener,
      (trans, service) => Trace.unwind {
        Trace.pushTracer(tracer)
        new mux.ServerDispatcher(trans, service, true)
      }
    )
    object TestThriftMuxServer extends ThriftMuxServerImpl(TestThriftMuxer)

    val testService = new TestService.FutureIface {
      def query(x: String) = Future.value(x + x)
    }
    val server = TestThriftMuxServer.serveIface(":*", testService)
    val client = Thrift.newIface[TestService.FutureIface](server)
    var p: Future[String] = null
    Trace.unwind {
      Trace.pushTracer(tracer)
      Trace.setId(TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), None))
      p = client.query("ok")
    }
    Await.result(p)

    (srvTraceId, cltTraceId) match {
      case (Some(id1), Some(id2)) => assert(id1 === id2)
      case _ => assert(false, "the trace ids sent by client and received by server do not match")
    }
  }

  test("thriftmux server + Finagle thrift client: clientId should be passed from client to server") {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(ClientId.current map { _.name } getOrElse(""))
    })

    val clientId = "test.service"
    val client = Thrift
      .withClientId(ClientId(clientId))
      .newIface[TestService.FutureIface](server)

    1 to 5 foreach { _ =>
      assert(Await.result(client.query("ok")) == clientId)
    }
  }

  test("thriftmux server + Finagle thrift client: ClientId should not be overridable externally") {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(ClientId.current map { _.name } getOrElse(""))
    })

    val clientId = ClientId("test.service")
    val otherClientId = ClientId("other.bar")
    val client = Thrift
      .withClientId(clientId)
      .newIface[TestService.FutureIface](server)

    1 to 5 foreach { _ =>
      otherClientId.asCurrent {
        assert(Await.result(client.query("ok")) == clientId.name)
      }
    }
  }

  test("thriftmux server + Finagle thrift client: server.close()") {
    new ThriftMuxTestServer {
      val client = Thrift.newIface[TestService.FutureIface](server)

      Await.result(client.query("ok"))
      1 to 5 foreach { _ => client.query("ok") }
      Await.result(server.close())
    }
  }

  test("thriftmux server + thriftmux client: ClientId should not be overridable externally") {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(ClientId.current map { _.name } getOrElse(""))
    })

    val clientId = ClientId("test.service")
    val otherClientId = ClientId("other.bar")
    val client = ThriftMux
      .withClientId(clientId)
      .newIface[TestService.FutureIface](server)

    1 to 5 foreach { _ =>
      otherClientId.asCurrent {
        assert(Await.result(client.query("ok")) == clientId.name)
      }
    }
  }

  object OldPlainThriftClient
    extends DefaultClient[ThriftClientRequest, Array[Byte]](
      name = "thrift",
      endpointer = Bridge[ThriftClientRequest, Array[Byte], ThriftClientRequest, Array[Byte]](
        ThriftFramedTransporter, new SerialClientDispatcher(_))
    ) with ThriftRichClient
  {
    protected val defaultClientName = "thrift"
    protected val protocolFactory = Protocols.binaryFactory()
  }

  test("thriftmux server + Finagle thrift client w/o protocol upgrade") {
    new ThriftMuxTestServer {
      val client = OldPlainThriftClient.newIface[TestService.FutureIface](server)
      1 to 5 foreach { _ =>
        assert(Await.result(client.query("ok")) == "okok")
      }
    }
  }

  test("thriftmux server + Finagle thrift client w/o protocol upgrade: server.close()") {
    new ThriftMuxTestServer {
      val client = OldPlainThriftClient.newIface[TestService.FutureIface](server)

      Await.result(client.query("ok"))
      1 to 5 foreach { _ => client.query("ok") }
      Await.result(server.close())
    }
  }

  test("""|thriftmux server + thrift client: client should receive a
         | TApplicationException if the server throws an unhandled exception
       """.stripMargin) {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = throw new Exception("sad panda")
    })
    val client = OldPlainThriftClient.newIface[TestService.FutureIface](server)
    val thrown = intercept[Exception] { Await.result(client.query("ok")) }
    assert(thrown.getMessage === "Internal error processing query: 'java.lang.Exception: sad panda'")
  }

  test("thriftmux server + thrift client w/o protocol upgrade but w/ pipelined dispatch") {
    val nreqs = 5
    val servicePromises = Array.fill(nreqs)(new Promise[String])
    val requestReceived = Array.fill(nreqs)(new Promise[String])
    val testService = new TestService.FutureIface {
      @volatile var nReqReceived = 0
      def query(x: String) = synchronized {
        nReqReceived += 1
        requestReceived(nReqReceived-1).setValue(x)
        servicePromises(nReqReceived-1)
      }
    }
    val server = ThriftMux.serveIface(":*", testService)

    object OldPlainThriftClient
      extends DefaultClient[ThriftClientRequest, Array[Byte]](
        name = "thrift",
        endpointer = Bridge[ThriftClientRequest, Array[Byte], ThriftClientRequest, Array[Byte]](
          ThriftFramedTransporter, new PipeliningDispatcher(_))
      )
    val service = Await.result(OldPlainThriftClient.newClient(server)())
    val client = new TestService.FinagledClient(service, Protocols.binaryFactory())
    val reqs = 1 to nreqs map { i => client.query("ok" + i) }
    // Although the requests are pipelined in the client, they must be
    // received by the service serially.
    1 to nreqs foreach { i =>
      val req = Await.result(requestReceived(i-1))
      if (i != nreqs) assert(!requestReceived(i).isDefined)
      assert(testService.nReqReceived === i)
      servicePromises(i-1).setValue(req + req)
    }
    1 to nreqs foreach { i =>
      assert(Await.result(reqs(i-1)) === "ok" + i + "ok" + i)
    }
  }

  test("thriftmux client: should emit ClientId") {
    val server = ThriftMux.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = {
        Future.value(ClientId.current.map(_.name).getOrElse(""))
      }
    })

    val client = ThriftMux.withClientId(ClientId("foo.bar"))
      .newIface[TestService.FutureIface](server)

    assert(Await.result(client.query("ok")) == "foo.bar")
  }

/* TODO: add back when sbt supports old-school thrift gen
  test("end-to-end finagle-thrift") {
    import com.twitter.finagle.thriftmux.thrift.TestService

    val server = ThriftMux.serveIface(":*", new TestService.ServiceIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.ServiceIface](server)
    assert(client.query("ok").get() == "okok")
  }
*/

  test("StackClient-based ThriftMux client properly creates clients") {
    new ThriftMuxTestServer {
      val client = exp.ThriftMuxClient.newIface[TestService.FutureIface](server)
      assert(Await.result(client.query("ok")) == "okok")
    }
  }

  test("StackServer-based ThriftMux server properly creates servers") {
    val server = exp.ThriftMuxServer.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })

    val client = ThriftMux.newIface[TestService.FutureIface](server)

    assert(Await.result(client.query("ok")) == "okok")
  }

  test("StackClient- and StackServer-based ThriftMux client/server can talk to eachother") {
    val server = exp.ThriftMuxServer.serveIface(":*", new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    })
    val client = exp.ThriftMuxClient.newIface[TestService.FutureIface](server)

    assert(Await.result(client.query("ok")) == "okok")
  }
}
