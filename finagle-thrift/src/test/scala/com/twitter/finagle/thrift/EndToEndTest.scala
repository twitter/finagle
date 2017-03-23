package com.twitter.finagle.thrift

import com.twitter.conversions.time._
import com.twitter.finagle.{Address, _}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.{InMemoryStatsReceiver, LoadedStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.service.ThriftResponseClassifier
import com.twitter.finagle.thrift.thriftscala._
import com.twitter.finagle.tracing.{Annotation, Record, Trace}
import com.twitter.finagle.util.HashedWheelTimer
import com.twitter.io.TempFile
import com.twitter.test._
import com.twitter.util._
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TCompactProtocol, TProtocolFactory}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with ThriftTest with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  type Iface = B.ServiceIface
  def ifaceManifest = implicitly[ClassTag[B.ServiceIface]]

  class BServiceImpl extends B.ServiceIface {
    def add(a: Int, b: Int) = Future.exception(new AnException)
    def add_one(a: Int, b: Int) = Future.Void
    def multiply(a: Int, b: Int) = Future { a * b }
    def complex_return(someString: String) = Future {
      Trace.record("hey it's me!")
      new SomeStruct(123, Trace.id.parentId.toString)
    }
    def someway() = Future.Void
    def show_me_your_dtab() = Future {
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      stringer.toString
    }

    def show_me_your_dtab_size() = Future {
      Dtab.local.length
    }
  }

  val processor = new BServiceImpl()

  val ifaceToService = new B.Service(_, _)
  val serviceToIface = new B.ServiceToClient(
    _: Service[ThriftClientRequest, Array[Byte]],
    _: TProtocolFactory,
    ResponseClassifier.Default)

  val missingClientIdEx = new IllegalStateException("uh no client id")
  val presentClientIdEx = new IllegalStateException("unexpected client id")

  def servers(pf: TProtocolFactory): Seq[(String, Closable, Int)] = {
    val iface = new BServiceImpl {
      override def show_me_your_dtab(): Future[String] = {
        ClientId.current.map(_.name) match {
          case Some(name) => Future.value(name)
          case _ => Future.exception(missingClientIdEx)
        }
      }
    }

    val builder = ServerBuilder()
      .name("server")
      .bindTo(new InetSocketAddress(0))
      .stack(Thrift.server.withProtocolFactory(pf))
      .build(ifaceToService(iface, pf))
    val proto = Thrift.server
      .withProtocolFactory(pf)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    def port(socketAddr: SocketAddress): Int =
      socketAddr.asInstanceOf[InetSocketAddress].getPort

    Seq(
      ("ServerBuilder", builder, port(builder.boundAddress)),
      ("Proto", proto, port(proto.boundAddress))
    )
  }

  def clients(
    pf: TProtocolFactory,
    clientId: Option[ClientId],
    port: Int
  ): Seq[(String, B.ServiceIface, Closable)] = {
    val dest = s"localhost:$port"

    var clientStack = Thrift.client.withProtocolFactory(pf)
    clientId.foreach { cId =>
      clientStack = clientStack.withClientId(cId)
    }

    val builder = ClientBuilder()
      .stack(clientStack)
      .dest(dest)
      .build()
    val proto = clientStack.newService(dest)

    def toIface(svc: Service[ThriftClientRequest, Array[Byte]]) =
      serviceToIface(svc, pf)

    Seq(
      ("ClientBuilder", toIface(builder), builder),
      ("Proto", toIface(proto), proto)
    )
  }

  // While we're supporting both old & new APIs, test the cross-product
  test("Mix of client and server creation styles") {
    for {
      clientId <- Seq(Some(ClientId("anClient")), None)
      pf <- Seq(Protocols.binaryFactory(), new TCompactProtocol.Factory())
      (serverWhich, serverClosable, port) <- servers(pf)
    } {
      for {
        (clientWhich, clientIface, clientClosable) <- clients(pf, clientId, port)
      } withClue(
        s"Server ($serverWhich) Client ($clientWhich) clientId $clientId protocolFactory $pf"
      ) {
        val resp = clientIface.show_me_your_dtab()
        clientId match {
          case Some(cId) =>
            assert(cId.name == Await.result(resp, 10.seconds))
          case None =>
            val ex = intercept[TApplicationException] { Await.result(resp, 10.seconds) }
            assert(ex.getMessage.contains(missingClientIdEx.toString))
        }
        clientClosable.close()
      }
      serverClosable.close()
    }
  }

  test("Exceptions are treated as failures") {
    val protocolFactory = Protocols.binaryFactory()

    val impl = new BServiceImpl {
      override def add(a: Int, b: Int) =
        Future.exception(new RuntimeException("lol"))
    }

    val sr = new InMemoryStatsReceiver()
    val server = Thrift.server
      .configured(Stats(sr))
      .serve(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        ifaceToService(impl, protocolFactory))
    val client = Thrift.client.newIface[B.ServiceIface](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    intercept[org.apache.thrift.TApplicationException] {
      Await.result(client.add(1, 2), 10.seconds)
    }

    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters.get(Seq("success")) == None)
    assert(sr.counters(Seq("failures")) == 1)
    server.close()
  }

  testThrift("unique trace ID") { (client, tracer) =>
    val f1 = client.add(1, 2)
    intercept[AnException] { Await.result(f1, 15.seconds) }
    val idSet1 = (tracer map (_.traceId.traceId)).toSet

    tracer.clear()

    val f2 = client.add(2, 3)
    intercept[AnException] { Await.result(f2, 15.seconds) }
    val idSet2 = (tracer map (_.traceId.traceId)).toSet

    assert(idSet1.nonEmpty)
    assert(idSet2.nonEmpty)

    assert(idSet1 != idSet2)
  }

  skipTestThrift("propagate Dtab") { (client, tracer) =>
    Dtab.unwind {
      Dtab.local = Dtab.read("/a=>/b; /b=>/$/inet/google.com/80")
      val clientDtab = Await.result(client.show_me_your_dtab(), 10.seconds)
      assert(clientDtab == "Dtab(2)\n\t/a => /b\n\t/b => /$/inet/google.com/80\n")
    }
  }

  testThrift("(don't) propagate Dtab") { (client, tracer) =>
    val dtabSize = Await.result(client.show_me_your_dtab_size(), 10.seconds)
    assert(dtabSize == 0)
  }

  test("JSON is broken (before we upgrade)") {
    // We test for the presence of a JSON encoding
    // bug in thrift 0.5.0[1]. See THRIFT-1375.
    //  When we upgrade, this test will fail and helpfully
    // remind us to add JSON back.
    import java.nio.ByteBuffer
    import org.apache.thrift.protocol._
    import org.apache.thrift.transport._

    val bytes = Array[Byte](102, 100, 125, -96, 57, -55, -72, 18,
      -21, 15, -91, -36, 104, 111, 111, -127, -21, 15, -91, -36,
      104, 111, 111, -127, 0, 0, 0, 0, 0, 0, 0, 0)
    val pf = new TJSONProtocol.Factory()

    val json = {
      val buf = new TMemoryBuffer(512)
      pf.getProtocol(buf).writeBinary(ByteBuffer.wrap(bytes))
      java.util.Arrays.copyOfRange(buf.getArray(), 0, buf.length())
    }

    val decoded = {
      val trans = new TMemoryInputTransport(json)
      val bin = pf.getProtocol(trans).readBinary()
      val bytes = new Array[Byte](bin.remaining())
      bin.get(bytes, 0, bin.remaining())
      bytes
    }

    assert(bytes.toSeq != decoded.toSeq, "Add JSON support back")
  }

  testThrift("end-to-end tracing potpourri") { (client, tracer) =>
    val id = Trace.nextId
    Trace.letId(id) {
      assert(Await.result(client.multiply(10, 30), 10.seconds) == 300)

      assert(tracer.nonEmpty)
      val idSet = tracer.map(_.traceId).toSet

      val ids = idSet.filter(_.traceId == id.traceId)
      assert(ids.size == 1)
      val theId = ids.head

      val traces: Seq[Record] = tracer
        .filter(_.traceId == theId)
        .filter {
          // Skip spurious GC messages
          case Record(_, _, Annotation.Message(msg), _) => !msg.startsWith("Gc")
          case _ => true
        }
        .toSeq

      // Verify the count of the annotations. Order may change.
      // These are set twice - by client and server
      assert(traces.collect { case Record(_, _, Annotation.BinaryAnnotation(k, v), _) => () }.size == 3)
      assert(traces.collect { case Record(_, _, Annotation.Rpc("multiply"), _) => () }.size == 2)
      assert(traces.collect { case Record(_, _, Annotation.ServerAddr(_), _) => () }.size == 2)
      // With Stack, we get an extra ClientAddr because of the
      // TTwitter upgrade request (ThriftTracing.CanTraceMethodName)
      assert(traces.collect { case Record(_, _, Annotation.ClientAddr(_), _) => () }.size >= 2)
      // LocalAddr is set on the server side only.
      assert(traces.collect { case Record(_, _, Annotation.LocalAddr(_), _) => () }.size == 1)
      // These are set by one side only.
      assert(traces.collect { case Record(_, _, Annotation.ServiceName("thriftclient"), _) => () }.size == 1)
      assert(traces.collect { case Record(_, _, Annotation.ServiceName("thriftserver"), _) => () }.size == 1)
      assert(traces.collect { case Record(_, _, Annotation.ClientSend(), _) => () }.size == 1)
      assert(traces.collect { case Record(_, _, Annotation.ServerRecv(), _) => () }.size == 1)
      assert(traces.collect { case Record(_, _, Annotation.ServerSend(), _) => () }.size == 1)
      assert(traces.collect { case Record(_, _, Annotation.ClientRecv(), _) => () }.size == 1)


      assert(Await.result(client.complex_return("a string"), 10.seconds).arg_two
        == "%s".format(Trace.id.spanId.toString))

      intercept[AnException] { Await.result(client.add(1, 2), 10.seconds) }
      Await.result(client.add_one(1, 2), 10.seconds)     // don't block!

      assert(Await.result(client.someway(), 10.seconds) == null)  // don't block!
    }
  }

  test("Configuring SSL over stack param") {
    def mkThriftTlsServer(sr: StatsReceiver) = {
      val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
      // deleteOnExit is handled by TempFile

      val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
      // deleteOnExit is handled by TempFile

      Thrift.server.withTransport.tls(SslServerConfiguration(
        keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile)))
        .configured(Stats(sr))
        .serve(
          new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
          ifaceToService(processor, Protocols.binaryFactory()))
    }

    def mkThriftTlsClient(server: ListeningServer) =
      Thrift.client.withTransport.tls(SslClientConfiguration(
        trustCredentials = TrustCredentials.Insecure))
        .newIface[B.ServiceIface](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client")

    val sr = new InMemoryStatsReceiver()

    val server = mkThriftTlsServer(sr)
    val client = mkThriftTlsClient(server)

    Await.result(client.multiply(1, 42), 15.seconds)

    assert(sr.counters(Seq("success")) == 1)

    server.close()
  }

  test("serveIface works with X.FutureIface, X[Future] with extended services") {
    // 1. Server extends X.FutureIface.
    class ExtendedEchoService1 extends ExtendedEcho.FutureIface {
      override def echo(msg: String): Future[String] = Future.value(msg)
      override def getStatus(): Future[String] = Future.value("OK")
    }

    val server1 = Thrift.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ExtendedEchoService1()
    )
    val client1 = Thrift.client.newIface[ExtendedEcho.FutureIface](Name.bound(Address(server1.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(Await.result(client1.echo("asdf"), 10.seconds) == "asdf")
    assert(Await.result(client1.getStatus(), 10.seconds) == "OK")

    // 2. Server extends X[Future].
    class ExtendedEchoService2 extends ExtendedEcho[Future] {
      override def echo(msg: String): Future[String] = Future.value(msg)
      override def getStatus(): Future[String] = Future.value("OK")
    }
    val server2 = Thrift.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ExtendedEchoService2()
    )
    val client2 = Thrift.client.newIface[ExtendedEcho.FutureIface](Name.bound(Address(server2.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(Await.result(client2.echo("asdf"), 10.seconds) == "asdf")
    assert(Await.result(client2.getStatus(), 10.seconds) == "OK")
  }

  runThriftTests()

  private val scalaClassifier: ResponseClassifier = {
    case ReqRep(Echo.Echo.Args(x), Throw(_: InvalidQueryException)) if x == "ok" =>
      ResponseClass.Success
    case ReqRep(_, Throw(_: InvalidQueryException)) => ResponseClass.NonRetryableFailure
    case ReqRep(_, Throw(_: RequestTimeoutException)) => ResponseClass.Success
    case ReqRep(_, Return(s: String)) => ResponseClass.NonRetryableFailure
  }

  private val javaClassifier: ResponseClassifier = {
    case ReqRep(x:thriftjava.Echo.echo_args, Throw(_: thriftjava.InvalidQueryException)) if x.msg == "ok" =>
      ResponseClass.Success
    case ReqRep(_, Throw(_: thriftjava.InvalidQueryException)) => ResponseClass.NonRetryableFailure
    case ReqRep(_, Return(s: String)) => ResponseClass.NonRetryableFailure
  }

  private def serverForClassifier(): ListeningServer  = {
    val iface = new Echo.FutureIface {
      def echo(x: String) =
        if (x == "safe")
          Future.value("safe")
        else if (x == "slow")
          Future.sleep(1.second)(HashedWheelTimer.Default).before(Future.value("slow"))
        else
          Future.exception(new InvalidQueryException(x.length))
    }
    val svc = new Echo.FinagledService(iface, Protocols.binaryFactory())
    Thrift.server
      .configured(Stats(NullStatsReceiver))
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)
  }

  private def testScalaFailureClassification(
     sr: InMemoryStatsReceiver,
     client: Echo.FutureIface
   ): Unit = {
    val ex = intercept[InvalidQueryException] {
      Await.result(client.echo("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      Await.result(client.echo("ok"), 5.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.echo("safe")))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)

    // this query produces a `Throw` response produced on the client side and
    // we want to ensure that we can translate it to a `Success`.
    intercept[RequestTimeoutException] {
      Await.result(client.echo("slow"), 10.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 4)
    assert(sr.counters(Seq("client", "success")) == 2)
  }

  private def testJavaFailureClassification(
    sr: InMemoryStatsReceiver,
    client: thriftjava.Echo.ServiceIface
  ): Unit = {
    val ex = intercept[thriftjava.InvalidQueryException] {
      Await.result(client.echo("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can examine the request as well.
    intercept[thriftjava.InvalidQueryException] {
      Await.result(client.echo("ok"), 5.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)
  }

  test("scala thrift stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds) // used in conjuection with a "slow" query
      .newIface[Echo.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    testScalaFailureClassification(sr, client)
    server.close()
  }

  test("java thrift stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withResponseClassifier(javaClassifier)
      .newIface[thriftjava.Echo.ServiceIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    testJavaFailureClassification(sr, client)
    server.close()
  }

  test("scala thrift ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(Thrift.client)
      .name("client")
      .reportTo(sr)
      .responseClassifier(scalaClassifier)
      .requestTimeout(100.milliseconds) // used in conjuection with a "slow" query
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = new Echo.FinagledClient(clientBuilder)

    testScalaFailureClassification(sr, client)
    server.close()
  }

  test("java thrift ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(Thrift.client)
      .name("client")
      .reportTo(sr)
      .responseClassifier(javaClassifier)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = new thriftjava.Echo.ServiceToClient(
      clientBuilder,
      Protocols.binaryFactory(),
      javaClassifier)

    testJavaFailureClassification(sr, client)
    server.close()
  }

  test("scala thrift client response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .newIface[Echo.FutureIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[InvalidQueryException] {
      Await.result(client.echo("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    server.close()
  }

  test("java thrift client response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .newIface[thriftjava.Echo.ServiceIface](Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val ex = intercept[thriftjava.InvalidQueryException] {
      Await.result(client.echo("hi"), 5.seconds)
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters.get(Seq("client", "success")) == None)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == Await.result(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    server.close()
  }

  test("Thrift server stats are properly scoped") {
    val iface: Echo.FutureIface = new Echo.FutureIface {
      def echo(x: String) =
        Future.value(x)
    }

    // Save loaded StatsReceiver
    val preSr = LoadedStatsReceiver.self

    val sr = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = sr

    val server = Thrift.server
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.newIface[Echo.FutureIface](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(Await.result(client.echo("hi"), 1.second) == "hi")
    assert(sr.counters(Seq("srv", "thrift", "echo", "requests")) == 1)
    assert(sr.counters(Seq("srv", "thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("srv", "requests")) == 1)

    server.close()

    // Restore previously loaded StatsReceiver
    LoadedStatsReceiver.self = preSr
  }

  private[this] val servers: Seq[(String, (StatsReceiver, Echo.FutureIface) => ListeningServer)] = Seq(
    "Thrift.server" ->
      ((sr, fi) => Thrift.server
        .withLabel("server")
        .withStatsReceiver(sr)
        .serve("localhost:*", new Echo.FinagledService(fi, Protocols.binaryFactory()))
      ),
    "ServerBuilder(stack)" ->
      ((sr, fi) => ServerBuilder().stack(Thrift.server)
        .name("server")
        .reportTo(sr)
        .bindTo(new InetSocketAddress(0))
        .build(new Echo.FinagledService(fi, Protocols.binaryFactory()))
      ),
    "ServerBuilder(codec)" ->
      ((sr, fi) => ServerBuilder().codec(ThriftServerFramedCodec())
        .name("server")
        .reportTo(sr)
        .bindTo(new InetSocketAddress(0))
        .build(new Echo.FinagledService(fi, Protocols.binaryFactory()))
      )
  )

  private[this] val clients: Seq[(String, (StatsReceiver, Address) => Echo.FutureIface)] = Seq(
    "Thrift.client" ->
      ((sr, addr) => Thrift.client
        .withStatsReceiver(sr)
        .newIface[Echo.FutureIface](Name.bound(addr), "client")
      ),
    "ClientBuilder(stack)" ->
      ((sr, addr) => new Echo.FinagledClient(ClientBuilder().stack(Thrift.client)
        .name("client")
        .hostConnectionLimit(1)
        .reportTo(sr)
        .dest(Name.bound(addr))
        .build())
      ),
    "ClientBuilder(codec)" ->
      ((sr, addr) => new Echo.FinagledClient(ClientBuilder().codec(ThriftClientFramedCodec())
        .name("client")
        .hostConnectionLimit(1)
        .reportTo(sr)
        .dest(Name.bound(addr))
        .build())
      )
  )

  for {
    (s, server) <- servers
    (c, client) <- clients
  } yield test(s"measures payload sizes: $s :: $c") {
    val sr = new InMemoryStatsReceiver

    val fi = new Echo.FutureIface {
      def echo(x: String) = Future.value(x + x)
    }

    val ss = server(sr, fi)
    val cc = client(sr, Address(ss.boundAddress.asInstanceOf[InetSocketAddress]))

    Await.ready(cc.echo("." * 10))



    // 40 bytes messages are from protocol negotiation made by TTwitter*Filter
    assert(sr.stat("client", "request_payload_bytes")() == Seq(40.0f, 209.0f))
    assert(sr.stat("client", "response_payload_bytes")() == Seq(40.0f, 45.0f))
    assert(sr.stat("server", "request_payload_bytes")() == Seq(40.0f, 209.0f))
    assert(sr.stat("server", "response_payload_bytes")() == Seq(40.0f, 45.0f))

    Await.ready(ss.close())
  }

  test("clientId is not sent and prep stats are not recorded when TTwitter upgrading is disabled") {
    val pf = Protocols.binaryFactory()
    val iface = new BServiceImpl {
      override def someway(): Future[Void] = {
        ClientId.current.map(_.name) match {
          case Some(name) => Future.exception(presentClientIdEx)
          case _ => Future.Void
        }
      }
    }
    val server = Thrift.server
      .withProtocolFactory(pf)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withProtocolFactory(pf)
      .withClientId(ClientId("aClient"))
      .withNoAttemptTTwitterUpgrade
      .newIface[B.ServiceIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client")

    assert(Await.result(client.someway(), timeout = 100.millis) == null)
    assert(sr.stats.get(Seq("codec_connection_preparation_latency_ms")) == None)
  }
}

/*

[1]
% diff -u /Users/marius/src/thrift-0.5.0-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java /Users/marius/pkg/thrift/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java
--- /Users/marius/src/thrift-0.5.0-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java	2013-09-16 12:17:53.000000000 -0700
+++ /Users/marius/pkg/thrift/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java	2013-09-05 20:20:07.000000000 -0700
@@ -313,7 +313,7 @@
   // Temporary buffer used by several methods
   private byte[] tmpbuf_ = new byte[4];

-  // Read a byte that must match b[0]; otherwise an exception is thrown.
+  // Read a byte that must match b[0]; otherwise an exception is thrown.
   // Marked protected to avoid synthetic accessor in JSONListContext.read
   // and JSONPairContext.read
   protected void readJSONSyntaxChar(byte[] b) throws TException {
@@ -331,7 +331,7 @@
       return (byte)((char)ch - '0');
     }
     else if ((ch >= 'a') && (ch <= 'f')) {
-      return (byte)((char)ch - 'a');
+      return (byte)((char)ch - 'a' + 10);
     }
     else {
       throw new TProtocolException(TProtocolException.INVALID_DATA,
@@ -346,7 +346,7 @@
       return (byte)((char)val + '0');
     }
     else {
-      return (byte)((char)val + 'a');
+      return (byte)((char)(val - 10) + 'a');
     }
   }

*/
