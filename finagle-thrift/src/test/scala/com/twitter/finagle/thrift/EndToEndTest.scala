package com.twitter.finagle.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.ssl.ClientAuth
import com.twitter.finagle.ssl.KeyCredentials
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.LoadedStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.SourceRole
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.thrift.{ClientId => FinagleClientId}
import com.twitter.finagle.thrift.service.ThriftResponseClassifier
import com.twitter.finagle.thrift.thriftscala._
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.TempFile
import com.twitter.scrooge
import com.twitter.test._
import com.twitter.util._
import java.io.PrintWriter
import java.io.StringWriter
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.{List => JList}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TTransport
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import scala.reflect.ClassTag
import org.scalatest.funsuite.AnyFunSuite

class EndToEndTest
    extends AnyFunSuite
    with ThriftTest
    with BeforeAndAfter
    with Eventually
    with IntegrationPatience {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  type Iface = B.ServiceIface
  def ifaceManifest = implicitly[ClassTag[B.ServiceIface]]

  class BServiceImpl extends B.ServiceIface {
    def add(a: Int, b: Int): Future[Integer] = Future.exception(new AnException)
    def add_one(a: Int, b: Int): Future[Void] = Future.Void
    def multiply(a: Int, b: Int): Future[Integer] = Future { a * b }
    def complex_return(someString: String): Future[SomeStruct] = Future {
      Trace.record("hey it's me!")
      new SomeStruct(123, Trace.id.parentId.toString)
    }
    def someway(): Future[Void] = Future.Void
    def show_me_your_dtab(): Future[String] = Future {
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      stringer.toString
    }

    def show_me_your_dtab_size(): Future[Integer] = Future {
      Dtab.local.length
    }

    override def mergeable_add(alist: JList[Integer]): Future[Integer] = Future.value(0)
  }

  val processor = new BServiceImpl()

  val ifaceToService = new B.Service(_: Iface, _: RichServerParam)
  val serviceToIface = new B.ServiceToClient(
    _: Service[ThriftClientRequest, Array[Byte]],
    _: TProtocolFactory,
    ResponseClassifier.Default
  )

  val missingClientIdEx = new IllegalStateException("uh no client id")
  val presentClientIdEx = new IllegalStateException("unexpected client id")

  def servers(serverParam: RichServerParam): Seq[(String, Closable, Int)] = {
    val iface = new BServiceImpl {
      override def show_me_your_dtab(): Future[String] = {
        FinagleClientId.current.map(_.name) match {
          case Some(name) => Future.value(name)
          case _ => Future.exception(missingClientIdEx)
        }
      }
    }

    val proto = Thrift.server
      .withProtocolFactory(serverParam.protocolFactory)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    def port(socketAddr: SocketAddress): Int =
      socketAddr.asInstanceOf[InetSocketAddress].getPort

    Seq(
      ("Proto", proto, port(proto.boundAddress))
    )
  }

  def clients(
    pf: TProtocolFactory,
    clientId: Option[FinagleClientId],
    port: Int
  ): Seq[(String, B.ServiceIface, Closable)] = {
    val dest = s"localhost:$port"

    var clientStack = Thrift.client.withProtocolFactory(pf)
    clientId.foreach { cId => clientStack = clientStack.withClientId(cId) }

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
      clientId <- Seq(Some(FinagleClientId("anClient")), None)
      pf <- Seq(Protocols.binaryFactory(), new TCompactProtocol.Factory())
      (serverWhich, serverClosable, port) <- servers(RichServerParam(pf))
    } {
      for {
        (clientWhich, clientIface, clientClosable) <- clients(pf, clientId, port)
      } withClue(
        s"Server ($serverWhich) Client ($clientWhich) clientId $clientId protocolFactory $pf"
      ) {
        val resp = clientIface.show_me_your_dtab()
        clientId match {
          case Some(cId) =>
            assert(cId.name == await(resp, 10.seconds))
          case None =>
            val ex = intercept[TApplicationException] { await(resp, 10.seconds) }
            assert(ex.getMessage.contains(missingClientIdEx.toString))
        }
        clientClosable.close()
      }
      serverClosable.close()
    }
  }

  test("Exceptions are treated as failures") {

    val impl = new BServiceImpl {
      override def add(a: Int, b: Int) =
        Future.exception(new RuntimeException("lol"))
    }

    val sr = new InMemoryStatsReceiver()
    val server = Thrift.server
      .configured(Stats(sr))
      .serve(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        ifaceToService(impl, RichServerParam())
      )
    val client = Thrift.client.build[B.ServiceIface](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    intercept[org.apache.thrift.TApplicationException] {
      await(client.add(1, 2), 10.seconds)
    }

    eventually {
      assert(sr.counters(Seq("requests")) == 1)
      assert(sr.counters(Seq("success")) == 0)
      assert(sr.counters(Seq("failures")) == 1)

      assert(
        sr.expressions.contains(
          ExpressionSchemaKey(
            "success_rate",
            Map(ExpressionSchema.Role -> SourceRole.Server.toString),
            Nil)))
    }

    server.close()
  }

  testThrift("unique trace ID") { (client, tracer) =>
    val f1 = client.add(1, 2)
    intercept[AnException] { await(f1, 15.seconds) }
    val idSet1 = (tracer map (_.traceId.traceId)).toSet

    tracer.clear()

    val f2 = client.add(2, 3)
    intercept[AnException] { await(f2, 15.seconds) }
    val idSet2 = (tracer map (_.traceId.traceId)).toSet

    assert(idSet1.nonEmpty)
    assert(idSet2.nonEmpty)

    assert(idSet1 != idSet2)
  }

  skipTestThrift("propagate Dtab") { (client, _) =>
    Dtab.unwind {
      Dtab.local = Dtab.read("/a=>/b; /b=>/$/inet/google.com/80")
      val clientDtab = await(client.show_me_your_dtab(), 10.seconds)
      assert(clientDtab == "Dtab(2)\n\t/a => /b\n\t/b => /$/inet/google.com/80\n")
    }
  }

  testThrift("(don't) propagate Dtab") { (client, _) =>
    val dtabSize = await(client.show_me_your_dtab_size(), 10.seconds)
    assert(dtabSize == 0)
  }

  testThrift("end-to-end tracing potpourri", Some(FinagleClientId("foo_client"))) {
    (client, tracer) =>
      val id = Trace.nextId
      Trace.letId(id) {
        assert(await(client.multiply(10, 30), 10.seconds) == 300)

        assert(tracer.nonEmpty)
        val idSet = tracer.map(_.traceId).toSet

        val ids = idSet.filter(_.traceId == id.traceId)
        assert(ids.size == 1)
        val theId = ids.head

        val traces: Seq[Record] = tracer
          .filter(_.traceId == theId)
          .filter {
            // Skip spurious GC messages
            case Record(_, _, Annotation.Message(msg), _) => !(msg == "GC Start" || msg == "GC End")
            case Record(_, _, Annotation.BinaryAnnotation(k, _), _) => !k.contains("payload")
            case _ => true
          }
          .toSeq

        // Verify the count of the annotations. Order may change.
        // These are set twice - by client and server
        assert(
          traces.collect {
            case r @ Record(_, _, Annotation.BinaryAnnotation(key, _), _)
                if !key.contains("offload_pool_size") =>
              r
          }.size == 15
        )
        assert(traces.collect { case Record(_, _, Annotation.ServerAddr(_), _) => () }.size == 2)
        // With Stack, we get an extra ClientAddr because of the
        // TTwitter upgrade request (ThriftTracing.CanTraceMethodName)
        assert(traces.collect { case Record(_, _, Annotation.ClientAddr(_), _) => () }.size >= 2)
        // LocalAddr is set on the server side only.
        assert(traces.collect { case Record(_, _, Annotation.LocalAddr(_), _) => () }.size == 1)
        // These are set by one side only.
        assert(
          traces.collect {
            case Record(_, _, Annotation.ServiceName("thriftclient"), _) => ()
          }.size == 1
        )
        assert(
          traces.collect {
            case Record(_, _, Annotation.ServiceName("thriftserver"), _) => ()
          }.size == 1
        )
        assert(traces.collect { case Record(_, _, Annotation.ClientSend, _) => () }.size == 1)
        assert(traces.collect { case Record(_, _, Annotation.ServerRecv, _) => () }.size == 1)
        assert(traces.collect { case Record(_, _, Annotation.ServerSend, _) => () }.size == 1)
        assert(traces.collect { case Record(_, _, Annotation.ClientRecv, _) => () }.size == 1)

        assert(traces.collectFirst {
          case Record(_, _, Annotation.BinaryAnnotation("srv/clientId", name: String), _) =>
            name
        }
          == Some("foo_client"))

        assert(
          await(client.complex_return("a string"), 10.seconds).arg_two
            == "%s".format(Trace.id.spanId.toString)
        )

        intercept[AnException] { await(client.add(1, 2), 10.seconds) }
        await(client.add_one(1, 2), 10.seconds) // don't block!

        assert(await(client.someway(), 10.seconds) == null) // don't block!
      }
  }

  test("Configuring SSL over stack param") {
    def mkThriftTlsServer(sr: StatsReceiver) = {
      val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
      // deleteOnExit is handled by TempFile

      val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
      // deleteOnExit is handled by TempFile

      val interFile = TempFile.fromResourcePath("/ssl/certs/intermediate.cert.pem")
      // deleteOnExit is handled by TempFile

      Thrift.server.withTransport
        .tls(
          SslServerConfiguration(
            clientAuth = ClientAuth.Needed,
            keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile),
            trustCredentials = TrustCredentials.CertCollection(interFile)
          )
        )
        .configured(Stats(sr))
        .serve(
          new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
          ifaceToService(processor, RichServerParam())
        )
    }

    def mkThriftTlsClient(server: ListeningServer) = {
      val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
      // deleteOnExit is handled by TempFile

      val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
      // deleteOnExit is handled by TempFile

      val interFile = TempFile.fromResourcePath("/ssl/certs/intermediate.cert.pem")
      // deleteOnExit is handled by TempFile

      Thrift.client.withTransport
        .tls(
          SslClientConfiguration(
            keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile),
            trustCredentials = TrustCredentials.CertCollection(interFile)
          )
        )
        .build[B.ServiceIface](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )
    }

    val sr = new InMemoryStatsReceiver()

    val server = mkThriftTlsServer(sr)
    val client = mkThriftTlsClient(server)

    await(client.multiply(1, 42), 15.seconds)

    assert(sr.counters(Seq("success")) == 1)

    server.close()
  }

  test("serveIface works with X.MethodPerEndpoint, X.MethodPerEndpoint with extended services") {
    // 1. Server extends X.MethodPerEndpoint.
    class ExtendedEchoService1 extends ExtendedEcho.MethodPerEndpoint {
      override def echo(msg: String): Future[String] = Future.value(msg)
      override def getStatus(): Future[String] = Future.value("OK")
    }

    val server1 = Thrift.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ExtendedEchoService1()
    )
    val client1 = Thrift.client.build[ExtendedEcho.MethodPerEndpoint](
      Name.bound(Address(server1.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    assert(await(client1.echo("asdf"), 10.seconds) == "asdf")
    assert(await(client1.getStatus(), 10.seconds) == "OK")

    // 2. Server extends X.MethodPerEndpoint.
    class ExtendedEchoService2 extends ExtendedEcho.MethodPerEndpoint {
      override def echo(msg: String): Future[String] = Future.value(msg)
      override def getStatus(): Future[String] = Future.value("OK")
    }
    val server2 = Thrift.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ExtendedEchoService2()
    )
    val client2 = Thrift.client.build[ExtendedEcho.MethodPerEndpoint](
      Name.bound(Address(server2.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    assert(await(client2.echo("asdf"), 10.seconds) == "asdf")
    assert(await(client2.getStatus(), 10.seconds) == "OK")
  }

  runThriftTests()

  private val scalaClassifier: ResponseClassifier = {
    case ReqRep(Echo.Echo.Args(x), Throw(_: InvalidQueryException)) if x == "ok" =>
      ResponseClass.Success
    case ReqRep(_, Throw(_: InvalidQueryException)) => ResponseClass.NonRetryableFailure
    case ReqRep(_, Throw(_: RequestTimeoutException)) |
        ReqRep(_, Throw(_: java.util.concurrent.TimeoutException)) =>
      ResponseClass.Success
    case ReqRep(_, Return(_: String)) => ResponseClass.NonRetryableFailure
  }

  private val javaClassifier: ResponseClassifier = {
    case ReqRep(x: thriftjava.Echo.echo_args, Throw(_: thriftjava.InvalidQueryException))
        if x.msg == "ok" =>
      ResponseClass.Success
    case ReqRep(_, Throw(_: thriftjava.InvalidQueryException)) => ResponseClass.NonRetryableFailure
    case ReqRep(_, Return(s: String)) => ResponseClass.NonRetryableFailure
  }

  private val iface = new Echo.MethodPerEndpoint {
    def echo(x: String): Future[String] =
      if (x == "safe")
        Future.value("safe")
      else if (x == "slow")
        Future.sleep(1.second)(DefaultTimer).before(Future.value("slow"))
      else if (x == "ignore")
        Future.const(Throw(Failure.ignorable("hello?")))
      else
        Future.exception(new InvalidQueryException(x.length))
  }

  private class EchoServiceImpl extends thriftjava.Echo.ServiceIface {
    def echo(x: String): Future[String] =
      if (x == "safe")
        Future.value("safe")
      else if (x == "slow")
        Future.sleep(1.second)(DefaultTimer).before(Future.value("slow"))
      else
        Future.exception(new thriftjava.InvalidQueryException(x.length))
  }

  private def serverForClassifier(): ListeningServer = {
    val svc = new Echo.FinagledService(iface, RichServerParam())
    Thrift.server
      .configured(Stats(NullStatsReceiver))
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)
  }

  // Create a client stack with the StatsFilter at the top to ensure that it is before the
  // the retry filter and won't count retries
  private def clientStackForClassifier(): Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] =
    Thrift.client.stack
      .remove(StatsFilter.role)
      .prepend(StatsFilter.module)

  private def testScalaClientResponseClassification(
    sr: InMemoryStatsReceiver,
    client: Echo.MethodPerEndpoint
  ): Unit = {
    val ex = intercept[InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters(Seq("client", "success")) == 0)
    assert(sr.counters(Seq("client", "failures")) == 1)

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      await(client.echo("ok"))
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "failures")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe")))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)
    assert(sr.counters(Seq("client", "failures")) == 2)

    // this query produces a `Throw` response produced on the client side and
    // we want to ensure that we can translate it to a `Success`.
    intercept[RequestTimeoutException] {
      await(client.echo("slow"), 10.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 4)
    assert(sr.counters(Seq("client", "success")) == 2)
    assert(sr.counters(Seq("client", "failures")) == 2)

    // This query makes the server throw an ignorable failure.
    intercept[TApplicationException] {
      await(client.echo("ignore"), 10.seconds)
    }
    assert(sr.counters(Seq("client", "requests")) == 5)
    assert(sr.counters(Seq("client", "success")) == 3)
    assert(sr.counters(Seq("client", "failures")) == 2)
  }

  private def testScalaServerResponseClassification(
    sr: InMemoryStatsReceiver,
    client: Echo.MethodPerEndpoint
  ): Unit = {
    val ex = intercept[InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 1)
    assert(!sr.counters.contains(Seq("thrift", "echo", "success")))
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 1)

    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)
    assert(sr.counters(Seq("failures")) == 1)

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      await(client.echo("ok"))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 1)

    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)
    assert(sr.counters(Seq("failures")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe")))
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 3)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 3)
    assert(sr.counters(Seq("success")) == 1)
    assert(sr.counters(Seq("failures")) == 2)

    // this query produces a Timeout exception in server side and it should be
    // translated to `Success`
    intercept[TApplicationException] {
      await(client.echo("slow"))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 4)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 4)
    assert(sr.counters(Seq("success")) == 2)
    assert(sr.counters(Seq("failures")) == 2)

    // This query makes the server throw an ignorable failure. This increments
    // the request count, but not the failure count.
    intercept[TApplicationException] {
      await(client.echo("ignore"), 10.seconds)
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 5)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 4)
    assert(sr.counters(Seq("success")) == 2)
    assert(sr.counters(Seq("failures")) == 2)
  }

  private def testJavaClientResponseClassification(
    sr: InMemoryStatsReceiver,
    client: thriftjava.Echo.ServiceIface
  ): Unit = {
    val ex = intercept[thriftjava.InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters(Seq("client", "success")) == 0)

    // test that we can examine the request as well.
    intercept[thriftjava.InvalidQueryException] {
      await(client.echo("ok"))
    }
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 3)
    assert(sr.counters(Seq("client", "success")) == 1)
  }

  private def testJavaServerResponseClassification(
    sr: InMemoryStatsReceiver,
    client: thriftjava.Echo.ServiceIface
  ): Unit = {
    val ex = intercept[thriftjava.InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)

    // test that we can examine the request as well.
    intercept[thriftjava.InvalidQueryException] {
      await(client.echo("ok"))
    }
    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("requests")) == 3)
    assert(sr.counters(Seq("success")) == 1)
  }

  test("scala thrift stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .withStack(clientStackForClassifier())
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(300.milliseconds) // used in conjunction with a "slow" query
      .build[Echo.MethodPerEndpoint](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    testScalaClientResponseClassification(sr, client)
    server.close()
  }

  test("scala thrift stack server using response classification with `serveIface`") {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds)
      .withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.build[Echo.MethodPerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    testScalaServerResponseClassification(sr, client)
    server.close()
  }

  test("scala thrift stack server using response classification with `serve`") {
    val sr = new InMemoryStatsReceiver()

    val svc = new Echo.FinagledService(
      iface,
      RichServerParam(
        serverStats = sr,
        responseClassifier = scalaClassifier,
        perEndpointStats = true
      )
    )

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds)
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)

    val client = Thrift.client.build[Echo.MethodPerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    testScalaServerResponseClassification(sr, client)
    server.close()
  }

  test(
    "scala thrift stack server using response classification with " +
      "`servicePerEndpoint[ServicePerEndpoint]`"
  ) {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds)
      .withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.servicePerEndpoint[Echo.ServicePerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val ex = intercept[InvalidQueryException] {
      await(client.echo(Echo.Echo.Args("hi")))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 1)
    assert(!sr.counters.contains(Seq("thrift", "echo", "success")))
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 1)

    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)
    assert(sr.counters(Seq("failures")) == 1)

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      await(client.echo(Echo.Echo.Args("ok")))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 1)

    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)
    assert(sr.counters(Seq("failures")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo(Echo.Echo.Args("safe"))))
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 3)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 3)
    assert(sr.counters(Seq("success")) == 1)
    assert(sr.counters(Seq("failures")) == 2)

    // this query produces a Timeout exception in server side and it should be
    // translated to `Success`
    intercept[TApplicationException] {
      await(client.echo(Echo.Echo.Args("slow")))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 4)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 4)
    assert(sr.counters(Seq("success")) == 2)
    assert(sr.counters(Seq("failures")) == 2)

    // This query makes the server throw an ignorable failure. This increments
    // the request count, but not the failure count.
    intercept[TApplicationException] {
      await(client.echo(Echo.Echo.Args("ignore")))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 5)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "failures")) == 2)

    assert(sr.counters(Seq("requests")) == 4)
    assert(sr.counters(Seq("success")) == 2)
    assert(sr.counters(Seq("failures")) == 2)

    server.close()
  }

  test(
    "scala thrift stack server using response classification with " +
      "`servicePerEndpoint[ReqRepServicePerEndpoint]`"
  ) {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(scalaClassifier)
      .withRequestTimeout(100.milliseconds)
      .withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.servicePerEndpoint[Echo.ReqRepServicePerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val ex = intercept[InvalidQueryException] {
      await(client.echo(scrooge.Request(Echo.Echo.Args("hi"))))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 1)
    assert(!sr.counters.contains(Seq("thrift", "echo", "success")))

    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)

    // test that we can examine the request as well.
    intercept[InvalidQueryException] {
      await(client.echo(scrooge.Request(Echo.Echo.Args("ok"))))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)

    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo(scrooge.Request(Echo.Echo.Args("safe")))).value)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 3)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)

    assert(sr.counters(Seq("requests")) == 3)
    assert(sr.counters(Seq("success")) == 1)

    // this query produces a Timeout exception in server side and it should be
    // translated to `Success`
    intercept[TApplicationException] {
      await(client.echo(scrooge.Request(Echo.Echo.Args("slow"))))
    }
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 4)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 2)

    assert(sr.counters(Seq("requests")) == 4)
    assert(sr.counters(Seq("success")) == 2)
    server.close()
  }

  test("java thrift stack client deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .withStack(clientStackForClassifier())
      .configured(Stats(sr))
      .withResponseClassifier(javaClassifier)
      .build[thriftjava.Echo.ServiceIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    testJavaClientResponseClassification(sr, client)
    server.close()
  }

  test("java thrift stack server deserialized response classification") {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(javaClassifier)
      .withRequestTimeout(100.milliseconds)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), new EchoServiceImpl)

    val client = Thrift.client.build[thriftjava.Echo.ServiceIface](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    testJavaServerResponseClassification(sr, client)
    server.close()
  }

  test("scala thrift ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(Thrift.client.withStack(clientStackForClassifier()))
      .name("client")
      .reportTo(sr)
      .responseClassifier(scalaClassifier)
      .requestTimeout(100.milliseconds) // used in conjunction with a "slow" query
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = new Echo.FinagledClient(clientBuilder, RichClientParam())

    testScalaClientResponseClassification(sr, client)
    server.close()
  }

  test("java thrift ClientBuilder deserialized response classification") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val clientBuilder = ClientBuilder()
      .stack(Thrift.client.withStack(clientStackForClassifier()))
      .name("client")
      .reportTo(sr)
      .responseClassifier(javaClassifier)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client =
      new thriftjava.Echo.ServiceToClient(clientBuilder, Protocols.binaryFactory(), javaClassifier)

    testJavaClientResponseClassification(sr, client)
    server.close()
  }

  test("scala thrift client response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .build[Echo.MethodPerEndpoint](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    val ex = intercept[InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters(Seq("client", "success")) == 0)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    server.close()
  }

  test("scala thrift server response classification using ThriftExceptionAsFailures") {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .withRequestTimeout(100.milliseconds)
      .withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.build[Echo.MethodPerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val ex = intercept[InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 1)
    assert(!sr.counters.contains(Seq("thrift", "echo", "success")))

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)
    assert(sr.counters(Seq("thrift", "echo", "requests")) == 2)
    assert(sr.counters(Seq("thrift", "echo", "success")) == 1)
    server.close()
  }

  test("java thrift client response classification using ThriftExceptionsAsFailures") {
    val server = serverForClassifier()
    val sr = new InMemoryStatsReceiver()
    val client = Thrift.client
      .configured(Stats(sr))
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .build[thriftjava.Echo.ServiceIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    val ex = intercept[thriftjava.InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("client", "requests")) == 1)
    assert(sr.counters(Seq("client", "success")) == 0)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("client", "requests")) == 2)
    assert(sr.counters(Seq("client", "success")) == 1)
    server.close()
  }

  test("java thrift server response classification using ThriftExceptionsAsFailures") {
    val sr = new InMemoryStatsReceiver()

    val server = Thrift.server
      .withStatsReceiver(sr)
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .withRequestTimeout(100.milliseconds)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), new EchoServiceImpl)

    val client = Thrift.client.build[thriftjava.Echo.ServiceIface](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val ex = intercept[thriftjava.InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(sr.counters(Seq("requests")) == 1)
    assert(sr.counters(Seq("success")) == 0)

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(sr.counters(Seq("requests")) == 2)
    assert(sr.counters(Seq("success")) == 1)
    server.close()
  }

  test("Thrift server stats are properly scoped") {
    val iface: Echo.MethodPerEndpoint = new Echo.MethodPerEndpoint {
      def echo(x: String) =
        Future.value(x)
    }

    // Save loaded StatsReceiver
    val preSr = LoadedStatsReceiver.self

    val sr = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = sr

    val server = Thrift.server.withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.build[Echo.MethodPerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    assert(await(client.echo("hi"), 1.second) == "hi")
    assert(sr.counters(Seq("srv", "thrift", "echo", "requests")) == 1)
    assert(sr.counters(Seq("srv", "thrift", "echo", "success")) == 1)
    assert(sr.counters(Seq("srv", "requests")) == 1)

    server.close()

    // Restore previously loaded StatsReceiver
    LoadedStatsReceiver.self = preSr
  }

  test("RichServerParam and RichClientParam are correctly composed") {

    val serverStats = new InMemoryStatsReceiver

    val server = Thrift.server
      .withMaxReusableBufferSize(15)
      .withStatsReceiver(serverStats)
      .withPerEndpointStats
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val clientStats = new InMemoryStatsReceiver

    val client = Thrift.client
      .withMaxReusableBufferSize(15)
      .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)
      .withStatsReceiver(clientStats)
      .withTReusableBufferFactory(RichClientParam.createThriftReusableBuffer(20))
      .build[Echo.MethodPerEndpoint](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    val ex = intercept[InvalidQueryException] {
      await(client.echo("hi"))
    }
    assert("hi".length == ex.errorCode)
    assert(clientStats.counters(Seq("client", "requests")) == 1)
    assert(clientStats.counters(Seq("client", "success")) == 0)
    assert(serverStats.counters(Seq("thrift", "echo", "requests")) == 1)
    assert(!serverStats.counters.contains(Seq("thrift", "echo", "success")))

    // test that we can mark a successfully deserialized result as a failure
    assert("safe" == await(client.echo("safe"), 10.seconds))
    assert(clientStats.counters(Seq("client", "requests")) == 2)
    assert(clientStats.counters(Seq("client", "success")) == 1)
    assert(serverStats.counters(Seq("thrift", "echo", "requests")) == 2)
    assert(serverStats.counters(Seq("thrift", "echo", "success")) == 1)
    server.close()

  }

  test("per-endpoint stats won't be recorded if not explicitly set enabled") {
    val iface: Echo.MethodPerEndpoint = new Echo.MethodPerEndpoint {
      def echo(x: String) =
        Future.value(x)
    }
    val sr = new InMemoryStatsReceiver
    val server = Thrift.server
      .withStatsReceiver(sr)
      .serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), iface)

    val client = Thrift.client.build[Echo.MethodPerEndpoint](
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    assert(await(client.echo("hi"), 1.second) == "hi")
    intercept[NoSuchElementException] {
      assert(sr.counters(Seq("thrift", "echo", "requests")) == 1)
    }
    assert(sr.counters(Seq("requests")) == 1)

    server.close()
  }

  private[this] val servers: Seq[
    (String, (StatsReceiver, Echo.MethodPerEndpoint) => ListeningServer)
  ] =
    Seq(
      "Thrift.server" ->
        ((sr, fi) =>
          Thrift.server
            .withLabel("server")
            .withStatsReceiver(sr)
            .serve("localhost:*", new Echo.FinagledService(fi, Protocols.binaryFactory())))
    )

  private[this] val clients: Seq[(String, (StatsReceiver, Address) => Echo.MethodPerEndpoint)] =
    Seq(
      "Thrift.client" ->
        ((sr, addr) =>
          Thrift.client
            .withStatsReceiver(sr)
            .build[Echo.MethodPerEndpoint](Name.bound(addr), "client")),
      "ClientBuilder(stack)" ->
        ((sr, addr) =>
          new Echo.FinagledClient(
            ClientBuilder()
              .stack(Thrift.client)
              .name("client")
              .hostConnectionLimit(1)
              .reportTo(sr)
              .dest(Name.bound(addr))
              .build()
          ))
    )

  for {
    (s, server) <- servers
    (c, client) <- clients
  } yield test(s"measures payload sizes: $s :: $c") {
    val sr = new InMemoryStatsReceiver

    val fi = new Echo.MethodPerEndpoint {
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
        FinagleClientId.current.map(_.name) match {
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
      .withClientId(FinagleClientId("aClient"))
      .withNoAttemptTTwitterUpgrade
      .build[B.ServiceIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    assert(await(client.someway(), 100.millis) == null)
    assert(sr.stats.get(Seq("codec_connection_preparation_latency_ms")) == None)
  }

  test("default protocolFactory should return TFinagleProtocol") {

    val clientParamDefault = RichClientParam()
    clientParamDefault.protocolFactory.getClass.getName.contains("finagle.thrift.Protocols") == true
  }

  test("TBinaryProtocol.Factory has all correct field") {
    val clientParamTBF = RichClientParam(new TBinaryProtocol.Factory(false, false))
    val strictReadField = clientParamTBF.protocolFactory.getClass.getDeclaredField("strictRead_")
    val strictWriteField = clientParamTBF.protocolFactory.getClass.getDeclaredField("strictWrite_")
    val readLimitField =
      clientParamTBF.protocolFactory.getClass.getDeclaredField("stringLengthLimit_")

    strictReadField.setAccessible(true)
    strictWriteField.setAccessible(true)
    readLimitField.setAccessible(true)

    strictReadField.get(clientParamTBF.protocolFactory) == false
    strictWriteField.get(clientParamTBF.protocolFactory) == false
    readLimitField.get(clientParamTBF.protocolFactory) == Protocols.NoLimit
  }

  test("TBinaryProtocol.Factory has right info after we set system property") {

    System.setProperty("org.apache.thrift.readLength", "1000")
    val clientParamSysProp = RichClientParam(new TBinaryProtocol.Factory(false, false))
    val strictReadField =
      clientParamSysProp.protocolFactory.getClass.getDeclaredField("strictRead_")
    val strictWriteField =
      clientParamSysProp.protocolFactory.getClass.getDeclaredField("strictWrite_")
    val readLimitField =
      clientParamSysProp.protocolFactory.getClass.getDeclaredField("stringLengthLimit_")

    strictReadField.setAccessible(true)
    strictWriteField.setAccessible(true)
    readLimitField.setAccessible(true)

    strictReadField.get(clientParamSysProp.protocolFactory) == false
    strictWriteField.get(clientParamSysProp.protocolFactory) == false
    readLimitField.get(clientParamSysProp.protocolFactory) == 1000
    System.clearProperty("org.apache.thrift.readLength")
  }

  private[this] val exceptionProtocol = new TProtocolFactory {
    override def getProtocol(trans: TTransport): TProtocol = {
      throw new Exception("Evidence to show when Rich[Server|Client]Params are passed through")
    }
  }

  test("verify using stack API's .with... works to set RichClientParams when using ClientBuilder") {
    val server =
      Thrift.server.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), processor)

    val builder = ClientBuilder()
      .stack(Thrift.client.withProtocolFactory(exceptionProtocol))
      .name("exception-client")
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = serviceToIface(builder, Protocols.binaryFactory())

    intercept[Exception] {
      await(client.show_me_your_dtab())
    }
    await(server.close())
  }

  test(
    "passing RichClientParams to the FinagleClient when creating a client with ClientBuilder works") {
    val server =
      Thrift.server.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), processor)

    val builder = ClientBuilder()
      .stack(Thrift.client)
      .name("exception-client")
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
      .build()
    val client = serviceToIface(builder, exceptionProtocol)

    intercept[Exception] {
      await(client.show_me_your_dtab())
    }
    await(server.close())
  }
}

/*

[1]
% diff -u /Users/marius/src/thrift-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java /Users/marius/pkg/thrift/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java
--- /Users/marius/src/thrift-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java	2013-09-16 12:17:53.000000000 -0700
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
