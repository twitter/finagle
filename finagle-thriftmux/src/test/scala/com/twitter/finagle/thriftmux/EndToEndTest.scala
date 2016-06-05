package com.twitter.finagle.thriftmux

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.param.{Label, Stats, Tracer => PTracer}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier, Retries, RetryBudget}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.finagle.thriftmux.service.ThriftMuxResponseClassifier
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.finagle.tracing._
import com.twitter.finagle.tracing.Annotation.{ClientSend, ServerRecv}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Closable, Duration, Future, Promise, Return, Throw, Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}

import com.twitter.finagle.util.HashedWheelTimer
import org.apache.thrift.protocol._
import org.apache.thrift.TApplicationException
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Tag}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    super.test(testName, testTags:_*) {
      mux.sessionFailureDetector.let("none") { f }
    }
  }

  // Used for testing ThriftMux's Context functionality. Duplicated from the
  // finagle-mux package as a workaround because you can't easily depend on a
  // test package in Maven.
  case class TestContext(buf: Buf)

  val testContext = new Contexts.broadcast.Key[TestContext]("com.twitter.finagle.mux.MuxContext") {
    def marshal(tc: TestContext) = tc.buf
    def tryUnmarshal(buf: Buf) = Return(TestContext(buf))
  }

  trait ThriftMuxTestServer {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) =
          (Contexts.broadcast.get(testContext), Dtab.local) match {
            case (None, Dtab.empty) =>
              Future.value(x+x)

            case (Some(TestContext(buf)), _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab) =>
              Future.value(dtab.show)
          }
      })
  }

  test("end-to-end thriftmux") {
    new ThriftMuxTestServer {
      val client = ThriftMux.client.newIface[TestService.FutureIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")
      assert(await(client.query("ok")) == "okok")

      await(server.close())
    }
  }

  test("end-to-end thriftmux: propagate Contexts") {
    new ThriftMuxTestServer {
      val client = ThriftMux.client.newIface[TestService.FutureIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(await(client.query("ok")) == "hello context world")
      }

      await(server.close())
    }
  }

  test("end-to-end thriftmux: propagate Dtab.local") {
    new ThriftMuxTestServer {
      val client =
        ThriftMux.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")

      Dtab.unwind {
        Dtab.local = Dtab.read("/foo=>/bar")
        assert(await(client.query("ok")) == "/foo=>/bar")
      }

      await(server.close())
    }
  }

  test("thriftmux server + Finagle thrift client") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")
      1 to 5 foreach { _ =>
        assert(await(client.query("ok")) == "okok")
      }

      await(server.close())
    }
  }

  test("end-to-end thriftmux server + Finagle thrift client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(await(client.query("ok")) == "hello context world")
      }

      await(server.close())
    }
  }

  test("end-to-end thriftmux server + Finagle thrift client: propagate Dtab.local") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")

      Dtab.unwind {
        Dtab.local = Dtab.read("/foo=>/bar")
        assert(await(client.query("ok")) == "/foo=>/bar")
      }

      await(server.close())
    }
  }

  // While we're supporting both old & new APIs, test the cross-product
  test("Mix of client and server creation styles") {
    val clientId = ClientId("test.service")

    def servers(pf: TProtocolFactory): Seq[(String, Closable, Int)] = {
      val iface = new TestService.FutureIface {
        def query(x: String) =
          if (x.isEmpty) Future.value(ClientId.current.map(_.name).getOrElse(""))
          else Future.value(x + x)
      }

      val pfSvc = new TestService$FinagleService(iface, pf)
      val builder = ServerBuilder()
        .stack(ThriftMux.server.withProtocolFactory(pf))
        .name("ThriftMuxServer")
        .bindTo(new InetSocketAddress(0))
        .build(pfSvc)
      val builderOld = ServerBuilder()
        .stack(ThriftMuxServer.withProtocolFactory(pf))
        .name("ThriftMuxServer")
        .bindTo(new InetSocketAddress(0))
        .build(pfSvc)
      val protoNew = ThriftMux.server
        .withProtocolFactory(pf)
        .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)
      val protoOld = ThriftMuxServer
        .withProtocolFactory(pf)
        .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

      def port(socketAddr: SocketAddress): Int =
        socketAddr.asInstanceOf[InetSocketAddress].getPort

      Seq(
        ("ServerBuilder deprecated", builderOld, port(builderOld.boundAddress)),
        ("ServerBuilder", builder, port(builder.boundAddress)),
        ("ThriftMux proto deprecated", protoOld, port(protoOld.boundAddress)),
        ("ThriftMux proto", protoOld, port(protoNew.boundAddress))
      )
    }

    def clients(
      pf: TProtocolFactory,
      port: Int
    ): Seq[(String, TestService$FinagleClient, Closable)] = {
      val dest = s"localhost:$port"
      val builder = ClientBuilder()
        .stack(ThriftMux.client.withClientId(clientId).withProtocolFactory(pf))
        .dest(dest)
        .build()
      val oldBuilder = ClientBuilder()
        .stack(ThriftMuxClient.withClientId(clientId).withProtocolFactory(pf))
        .dest(dest)
        .build()
      val thriftBuilder = ClientBuilder()
        .codec(ThriftClientFramedCodec(Some(clientId)).protocolFactory(pf))
        .hostConnectionLimit(1)
        .dest(dest)
        .build()
      val thriftProto = Thrift.client
        .withClientId(clientId)
        .withProtocolFactory(pf)
        .newService(dest)
      val newProto = ThriftMux.client
        .withClientId(clientId)
        .withProtocolFactory(pf)
        .newService(dest)
      val oldProto = ThriftMuxClient
        .withClientId(clientId)
        .withProtocolFactory(pf)
        .newService(dest)

      def toIface(svc: Service[ThriftClientRequest, Array[Byte]]): TestService$FinagleClient =
        new TestService.FinagledClient(svc, pf)

      Seq(
        ("ThriftMux via ClientBuilder", toIface(builder), builder),
        ("ThriftMux via deprecated ClientBuilder", toIface(oldBuilder), oldBuilder),
        ("Thrift via ClientBuilder", toIface(thriftBuilder), thriftBuilder),
        ("Thrift via proto", toIface(thriftProto), thriftProto),
        ("ThriftMux proto deprecated", toIface(oldProto), oldProto),
        ("ThriftMux proto", toIface(newProto), newProto)
      )
    }

    for {
      pf <- Seq(new TCompactProtocol.Factory, Protocols.binaryFactory())
      (serverWhich, serverClosable, port) <- servers(pf)
    } {
      for {
        (clientWhich, clientIface, clientClosable) <- clients(pf, port)
      } withClue(s"Server ($serverWhich), Client ($clientWhich) client with protocolFactory $pf") {
        1.to(5).foreach { _ => assert(await(clientIface.query("ok")) == "okok") }
        assert(await(clientIface.query("")) == clientId.name)
        await(clientClosable.close())
      }
      await(serverClosable.close())
    }
  }

  test("thriftmux server + Finagle thrift client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client = Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(await(client.query("ok")) == "hello context world")
      }

      await(server.close())
    }
  }

  test("thriftmux server + Finagle thrift client: client should receive a " +
    "TApplicationException if the server throws an unhandled exception") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = throw new Exception("sad panda")
      })
    val client = Thrift.client.newIface[TestService.FutureIface](server)
    val thrown = intercept[TApplicationException] { await(client.query("ok")) }
    assert(thrown.getMessage == "Internal error processing query: 'java.lang.Exception: sad panda'")

    await(server.close())
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

    val server = ThriftMux.server
      .configured(PTracer(tracer))
      .serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        new TestService.FutureIface {
          def query(x: String) = Future.value(x + x)
        })

    val client = Thrift.client
      .configured(PTracer(tracer))
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    await(client.query("ok"))

    (srvTraceId, cltTraceId) match {
      case (Some(id1), Some(id2)) => assert(id1 == id2)
      case _ => assert(false, s"the trace ids sent by client and received by server do not match srv: $srvTraceId clt: $cltTraceId")
    }

    await(server.close())
  }

  test("thriftmux server + Finagle thrift client: clientId should be passed from client to server") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(ClientId.current.map(_.name).getOrElse(""))
      })

    val clientId = "test.service"
    val client = Thrift.client
      .withClientId(ClientId(clientId))
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    1 to 5 foreach { _ =>
      assert(await(client.query("ok")) == clientId)
    }

    await(server.close())
  }

  test("thriftmux server + Finagle thrift client: ClientId should not be overridable externally") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(ClientId.current.map(_.name).getOrElse(""))
      })

    val clientId = ClientId("test.service")
    val otherClientId = ClientId("other.bar")
    val client = Thrift.client
      .withClientId(clientId)
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    1 to 5 foreach { _ =>
      otherClientId.asCurrent {
        assert(await(client.query("ok")) == clientId.name)
      }
    }

    await(server.close())
  }

  test("thriftmux server + Finagle thrift client: server.close()") {
    new ThriftMuxTestServer {
      val client = Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")
      await(server.close())

      // This request fails and is not requeued because there are
      // no Open service factories in the load balancer.
      try await(client.query("ok")) catch {
        case _: ChannelClosedException => // ok
        case _: ChannelWriteException => // ok
      }

      // Subsequent requests are failed fast since there are (still) no
      // Open service factories in the load balancer. Again, no requeues
      // are attempted.
      intercept[FailedFastException] {
        await(client.query("ok"))
      }
    }
  }

  test("thriftmux server + thriftmux client: ClientId should not be overridable externally") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(ClientId.current.map(_.name).getOrElse(""))
      })

    val clientId = ClientId("test.service")
    val otherClientId = ClientId("other.bar")
    val client = ThriftMux.client
      .withClientId(clientId)
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    1 to 5 foreach { _ =>
      otherClientId.asCurrent {
        assert(await(client.query("ok")) == clientId.name)
      }
    }

    await(server.close())
  }

  // Skip upnegotiation.
  object OldPlainThriftClient extends Thrift.Client(stack=StackClient.newStack)

  test("thriftmux server + Finagle thrift client w/o protocol upgrade") {
    new ThriftMuxTestServer {
      val client = OldPlainThriftClient.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")
      1 to 5 foreach { _ =>
        assert(await(client.query("ok")) == "okok")
      }

      await(server.close())
    }
  }

  test("thriftmux server + Finagle thrift client w/o protocol upgrade: server.close()") {
    new ThriftMuxTestServer {
      val client = OldPlainThriftClient.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      assert(await(client.query("ok")) == "okok")
      await(server.close())
      intercept[ChannelWriteException] {
        await(client.query("ok"))
      }

      intercept[FailedFastException] {
        await(client.query("ok"))
      }

      await(server.close())
    }
  }

  test("thriftmux server + thrift client: client should receive a " +
    "TApplicationException if the server throws an unhandled exception") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = throw new Exception("sad panda")
      })
    val client = OldPlainThriftClient.newIface[TestService.FutureIface](server)
    val thrown = intercept[TApplicationException] { await(client.query("ok")) }
    assert(thrown.getMessage == "Internal error processing query: 'java.lang.Exception: sad panda'")
  }

  test("thriftmux server should count exceptions as failures") {
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.exception(new RuntimeException("lolol"))
    }
    val svc = new TestService.FinagledService(iface, Protocols.binaryFactory())

    val sr = new InMemoryStatsReceiver()
    val server = ThriftMux.server
      .configured(Stats(sr))
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)
    val client =
      ThriftMux.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[TApplicationException] {
      await(client.query("hi"))
    }
    assert(ex.getMessage.contains("lolol"))
    assert(sr.counters(Seq("thrift", "requests")) == 1)
    assert(sr.counters.get(Seq("thrift", "success")) == None)
    assert(sr.counters(Seq("thrift", "failures")) == 1)

    await(server.close())
  }

  test("thriftmux client default failure classification") {
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.exception(new InvalidQueryException(x.length))
    }
    val svc = new TestService.FinagledService(iface, Protocols.binaryFactory())

    val server = ThriftMux.server
      .configured(Stats(NullStatsReceiver))
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)

    val sr = new InMemoryStatsReceiver()
    val client =
      ThriftMux.client
        .configured(Stats(sr))
        .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[InvalidQueryException] {
      await(client.query("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    // by default, the filter/service are Array[Byte] => Array[Byte]
    // which in turn means thrift exceptions are just "successful"
    // arrays of bytes...
    assert(sr.counters(Seq("client", "success")) == 1)

    await(server.close())
  }

  private val scalaClassifier: ResponseClassifier =
    ResponseClassifier.named("EndToEndTestClassifier") {
      case ReqRep(TestService.Query.Args(x), Throw(_: InvalidQueryException)) if x == "ok" =>
        ResponseClass.Success
      case ReqRep(_, Throw(_: InvalidQueryException)) =>
        ResponseClass.NonRetryableFailure
      case ReqRep(_, Throw(_: RequestTimeoutException)) =>
        ResponseClass.Success
      case ReqRep(_, Return(s: String)) =>
        ResponseClass.NonRetryableFailure
  }

  private val javaClassifier: ResponseClassifier = {
    case ReqRep(x:thriftjava.TestService.query_args, Throw(_: thriftjava.InvalidQueryException)) if x.x == "ok" =>
      ResponseClass.Success
    case ReqRep(_, Throw(_: thriftjava.InvalidQueryException)) =>
      ResponseClass.NonRetryableFailure
    case ReqRep(_, Return(s: String)) =>
      ResponseClass.NonRetryableFailure
  }

  def serverForClassifier(): ListeningServer  = {
    val iface = new TestService.FutureIface {
      def query(x: String) =
        if (x == "safe")
          Future.value("safe")
        else if (x == "slow")
          Future.sleep(1.second)(HashedWheelTimer.Default).before(Future.value("slow"))
        else
          Future.exception(new InvalidQueryException(x.length))
    }
    val svc = new TestService.FinagledService(iface, Protocols.binaryFactory())
    ThriftMux.server
      .configured(Stats(NullStatsReceiver))
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)
  }

  private def testScalaFailureClassification(
    sr: InMemoryStatsReceiver,
    client: TestService.FutureIface
  ): Unit = {
    val ex = intercept[InvalidQueryException] {
      await(client.query("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)
    assert(sr.counters(Seq("client", "query", "requests")) == 1)
    eventually {
      assert(sr.counters(Seq("client", "query", "failures")) == 1)
      assert(sr.counters.get(Seq("client", "query", "success")) == None)
    }

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      await(client.query("ok"))
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "query", "requests")) == 2)
    eventually {
      assert(sr.counters(Seq("client", "query", "success")) == 1)
      assert(sr.counters(Seq("client", "query", "failures")) == 1)
    }

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.query("safe")))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "query", "requests")) == 3)
    eventually {
      assert(sr.counters(Seq("client", "query", "success")) == 1)
      assert(sr.counters(Seq("client", "query", "failures")) == 2)
    }

    // this query produces a `Throw` response produced on the client side and
    // we want to ensure that we can translate it to a `Success`.
    intercept[RequestTimeoutException] {
      await(client.query("slow"))
    }
    assert(sr.counters(Seq("client", "requests")) == 4)
    assert(sr.counters(Seq("client", "success")) == 2)
    assert(sr.counters(Seq("client", "query", "requests")) == 4)
    eventually {
      assert(sr.counters(Seq("client", "query", "success")) == 2)
      assert(sr.counters(Seq("client", "query", "failures")) == 2)
    }
  }

  private def testJavaFailureClassification(
    sr: InMemoryStatsReceiver,
    client: thriftjava.TestService.ServiceIface
  ): Unit = {
    val ex = intercept[thriftjava.InvalidQueryException] {
      Await.result(client.query("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can examine the request as well.
    intercept[thriftjava.InvalidQueryException] {
      Await.result(client.query("ok"), 5.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.query("safe")))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "failures")) == 2)
  }

  test("scala thriftmux stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds) // used in conjuection with a "slow" query
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    testScalaFailureClassification(sr, client)
    server.close()
  }

  test("java thriftmux stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .withStatsReceiver(sr)
      .withResponseClassifier(javaClassifier)
      .newIface[thriftjava.TestService.ServiceIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    testJavaFailureClassification(sr, client)
    server.close()
  }

  test("scala thriftmux ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(ThriftMux.client)
      .name("client")
      .reportTo(sr)
      .responseClassifier(scalaClassifier)
      .requestTimeout(100.milliseconds) // used in conjuection with a "slow" query
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = new TestService.FinagledClient(
      clientBuilder,
      serviceName = "client",
      stats = sr,
      responseClassifier = scalaClassifier
    )

    testScalaFailureClassification(sr, client)
    server.close()
  }

  test("java thriftmux ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(ThriftMux.client)
      .name("client")
      .reportTo(sr)
      .responseClassifier(javaClassifier)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = new thriftjava.TestService.ServiceToClient(
      clientBuilder,
      Protocols.binaryFactory(),
      javaClassifier)

    testJavaFailureClassification(sr, client)
    server.close()
  }

  test("scala thriftmux response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftMuxResponseClassifier.ThriftExceptionsAsFailures)
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[InvalidQueryException] {
      await(client.query("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.query("safe")))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    await(server.close())
  }

  test("java thriftmux response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftMuxResponseClassifier.ThriftExceptionsAsFailures)
      .newIface[thriftjava.TestService.ServiceIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[thriftjava.InvalidQueryException] {
      Await.result(client.query("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.query("safe")))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "failures")) == 1)
    server.close()
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
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0), testService)

    object OldPlainPipeliningThriftClient extends Thrift.Client(stack=StackClient.newStack) {
      override protected def newDispatcher(transport: Transport[ThriftClientRequest, Array[Byte]]) =
        new PipeliningDispatcher(transport)
    }

    val service = await(OldPlainPipeliningThriftClient.newClient(server)())
    val client = new TestService.FinagledClient(service, Protocols.binaryFactory())
    val reqs = 1 to nreqs map { i => client.query("ok" + i) }
    // Although the requests are pipelined in the client, they must be
    // received by the service serially.
    1 to nreqs foreach { i =>
      val req = await(requestReceived(i-1))
      if (i != nreqs) assert(!requestReceived(i).isDefined)
      assert(testService.nReqReceived == i)
      servicePromises(i-1).setValue(req + req)
    }
    1 to nreqs foreach { i =>
      assert(await(reqs(i-1)) == "ok" + i + "ok" + i)
    }
    await(server.close())
  }

  test("thriftmux client: should emit ClientId") {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = {
          Future.value(ClientId.current.map(_.name).getOrElse(""))
        }
      })

    val client = ThriftMux.client.withClientId(ClientId("foo.bar"))
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(await(client.query("ok")) == "foo.bar")
    await(server.close())
  }

  /* TODO: add back when sbt supports old-school thrift gen
   test("end-to-end finagle-thrift") {
     import com.twitter.finagle.thriftmux.thrift.TestService

     val server = ThriftMux.servver.serveIface(
       new InetSocketAddress(InetAddress.getLoopbackAddress, 0), new TestService.ServiceIface {
         def query(x: String) = Future.value(x+x)
       })

     val client = ThriftMux.client.newIface[TestService.ServiceIface](server)
     assert(client.query("ok").get() == "okok")
   }
   */

  test("ThriftMux servers and clients should export protocol stats") {
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.value(x+x)
    }
    val mem = new InMemoryStatsReceiver
    val sr = Stats(mem)
    val server = ThriftMux.server
      .configured(sr)
      .configured(Label("server"))
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = ThriftMux.client
      .configured(sr)
      .configured(Label("client"))
      .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(await(client.query("ok")) == "okok")
    assert(mem.gauges(Seq("server", "protocol", "thriftmux"))() == 1.0)
    assert(mem.gauges(Seq("client", "protocol", "thriftmux"))() == 1.0)
    await(server.close())
  }

  test("ThriftMux clients are properly labeled and scoped") {
    new ThriftMuxTestServer {
      def base(sr: InMemoryStatsReceiver) = ThriftMux.Client().configured(Stats(sr))

      def assertStats(prefix: String, sr: InMemoryStatsReceiver, iface: TestService.FutureIface) {
        assert(await(iface.query("ok")) == "okok")
        // These stats are exported by scrooge generated code.
        assert(sr.counters(Seq(prefix, "query", "requests")) == 1)
      }

      // non-labeled client inherits destination as label
      val sr1 = new InMemoryStatsReceiver
      assertStats(server.toString, sr1, base(sr1).newIface[TestService.FutureIface](server))

      // labeled via configured
      val sr2 = new InMemoryStatsReceiver
      assertStats("client", sr2,
        base(sr2).newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client"))

      await(server.close())
    }
  }

  test("downgraded pipelines are properly scoped") {
    val sr = new InMemoryStatsReceiver

    val server = ThriftMux.server
      .configured(Stats(sr))
      .configured(Label("myserver"))
      .serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        new TestService.FutureIface {
          def query(x: String) = Future.value(x+x)
        })

    val thriftClient =
      Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(await(thriftClient.query("ok")) == "okok")
    assert(sr.counters(Seq("myserver", "thriftmux", "downgraded_connects")) == 1)

    await(server.close())
  }

  test("ThriftMux.server with TCompactProtocol") {
    val pf = new TCompactProtocol.Factory
    val server = ThriftMux.server.withProtocolFactory(pf).serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(x+x)
      })

    val dst = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val tcompactClient = ThriftMux.client.withProtocolFactory(pf)
      .newIface[TestService.FutureIface](dst, "client")
    assert(await(tcompactClient.query("ok")) == "okok")

    await(server.close())
  }

  test("ThriftMuxServer with TCompactProtocol") {
    val pf = new TCompactProtocol.Factory
    val server = ThriftMuxServer.withProtocolFactory(pf).serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(x+x)
      })

    val dst = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val tcompactClient = ThriftMux.client.withProtocolFactory(pf)
      .newIface[TestService.FutureIface](dst, "client")
    assert(await(tcompactClient.query("ok")) == "okok")

    await(server.close())
  }

  test("ThriftMux.server with TCompactProtocol: binary fails") {
    val server = ThriftMux.server.withProtocolFactory(new TCompactProtocol.Factory).serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(x+x)
      })

    val dst = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val tbinaryClient = ThriftMux.client.newIface[TestService.FutureIface](dst, "client")
    intercept[com.twitter.finagle.mux.ServerApplicationError] {
      await(tbinaryClient.query("ok"))
    }

    await(server.close())
  }

  test("ThriftMuxServer with TCompactProtocol: binary fails") {
    val server = ThriftMuxServer.withProtocolFactory(new TCompactProtocol.Factory).serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String) = Future.value(x+x)
      })

    val dst = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val tbinaryClient = ThriftMux.client.newIface[TestService.FutureIface](dst, "client")
    intercept[com.twitter.finagle.mux.ServerApplicationError] {
      await(tbinaryClient.query("ok"))
    }
  }

  test("ThriftMux client to Thrift server ") {
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.value(x + x)
    }
    val mem = new InMemoryStatsReceiver
    val sr = Stats(mem)
    val server = Thrift.server
      .configured(sr)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort
    val clientSvc = ThriftMux.client
      .configured(sr)
      .configured(Label("client"))
      .newService(s"localhost:$port")
    val client = new TestService.FinagledClient(clientSvc)

    // the thrift server doesn't understand the protocol of the request,
    // so it does its usual thing and closes the connection.
    intercept[ChannelClosedException] {
      await(client.query("ethics"))
    }

    await(clientSvc.close() join server.close())
  }

  test("drain downgraded connections") {
    val response = new Promise[String]()
    val iface = new TestService.FutureIface {
      def query(x: String): Future[String] = response
    }

    val inet = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ThriftMux.server.serveIface(inet, iface)
    val client =
      Thrift.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val f = client.query("ok")
    intercept[Exception] { await(f) }

    val close = server.close(1.minute) // up to a minute
    intercept[Exception] { await(close) }

    response.setValue("done")

    assert(await(close.liftToTry) == Return.Unit)
    assert(await(f) == "done")

    await(server.close())
  }

  test("gracefully reject sessions") {
    @volatile var n = 0
    val server = ThriftMux.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ServiceFactory {
        def apply(conn: ClientConnection) = {
          n += 1
          Future.exception(new Exception)
        }
        def close(deadline: Time) = Future.Done
      })

    val client =
      ThriftMux.client.newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val failure = intercept[Failure] {
      await(client.query("ok"))
    }

    // Failure.Restartable is stripped.
    assert(!failure.isFlagged(Failure.Restartable))

    // Tried multiple times.
    assert(n > 1)

    await(server.close())
  }

  trait ThriftMuxFailServer {
    val serverSr = new InMemoryStatsReceiver
    val server =
      ThriftMux.server
        .configured(Stats(serverSr))
        .serveIface(
          new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
          new TestService.FutureIface {
            def query(x: String) = Future.exception(Failure.rejected("unhappy"))
          })
  }

  /** no minimum, and 1 request gives 1 retry */
  def budget: RetryBudget =
    RetryBudget(1.minute, minRetriesPerSec = 0, percentCanRetry = 1.0)

  test("thriftmux server + thriftmux client: auto requeues retryable failures") {
    new ThriftMuxFailServer {
      val sr = new InMemoryStatsReceiver
      val client =
        ThriftMux.client
          .configured(Stats(sr))
          .configured(Retries.Budget(budget))
          .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      val failure = intercept[Exception](await(client.query("ok")))
      assert(failure.getMessage ==  "The request was Nacked by the server")

      assert(serverSr.counters(Seq("thrift", "thriftmux", "connects")) == 1)
      assert(serverSr.counters(Seq("thrift", "requests")) == 2)
      assert(serverSr.counters(Seq("thrift", "failures")) == 2)

      assert(sr.counters(Seq("client", "query", "requests")) == 1)
      assert(sr.counters(Seq("client", "requests")) == 2)
      assert(sr.counters(Seq("client", "failures")) == 2)

      // reuse connection
      intercept[Exception](await(client.query("ok")))
      assert(serverSr.counters(Seq("thrift", "thriftmux", "connects")) == 1)

      await(server.close())
    }
  }

  test("thriftmux server + thrift client: does not support Nack") {
    new ThriftMuxFailServer {
      val sr = new InMemoryStatsReceiver
      val client =
        Thrift.client
          .configured(Stats(sr))
          .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      val failure = intercept[ChannelClosedException](await(client.query("ok")))
      assert(failure.getMessage.startsWith("ChannelException at remote address"))

      assert(serverSr.counters(Seq("thrift", "requests")) == 1)
      assert(serverSr.counters(Seq("thrift", "connects")) == 1)
      assert(serverSr.counters(Seq("thrift", "thriftmux", "downgraded_connects")) == 1)

      assert(sr.counters(Seq("client", "requests")) == 1)
      assert(sr.counters(Seq("client", "failures")) == 1)
      assert(sr.stats(Seq("client", "connection_duration")).size == 1)
      assert(sr.counters.get(Seq("client", "retries", "requeues")) == None)

      intercept[ChannelClosedException](await(client.query("ok")))
      // reconnects on the second request
      assert(serverSr.counters(Seq("thrift", "connects")) == 2)

      await(server.close())
    }
  }

  trait ThriftMuxFailSessionServer {
    val serverSr = new InMemoryStatsReceiver
    val server =
      ThriftMux.server
        .configured(Stats(serverSr))
        .serve(
          new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
          new ServiceFactory {
            def apply(conn: ClientConnection) = Future.exception(new Exception("unhappy"))
            def close(deadline: Time) = Future.Done
          }
        )
  }

  test("thriftmux server + thriftmux client: session rejection") {
    new ThriftMuxFailSessionServer {
      val sr = new InMemoryStatsReceiver
      val client =
        ThriftMux.client
          .configured(Stats(sr))
          .configured(Retries.Budget(budget))
          .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      val failure = intercept[Exception](await(client.query("ok")))
      assert(failure.getMessage == "The request was Nacked by the server")

      assert(serverSr.counters(Seq("thrift", "mux", "draining")) >= 1)

      assert(sr.counters(Seq("client", "retries", "requeues")) == 2 - 1)
      assert(sr.counters(Seq("client", "requests")) == 2)
      assert(sr.counters(Seq("client", "failures")) == 2)

      await(server.close())
    }
  }

  test("thriftmux server + thrift client: session rejection") {
    new ThriftMuxFailSessionServer {
      val sr = new InMemoryStatsReceiver
      val client =
        Thrift.client
          .configured(Stats(sr))
          .newIface[TestService.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      intercept[ChannelClosedException](await(client.query("ok")))
      assert(sr.counters.get(Seq("client", "retries", "requeues")) == None)
      assert(sr.counters(Seq("client", "requests")) == 1)
      assert(sr.counters(Seq("client", "failures")) == 1)

      await(server.close())
    }
  }
}
