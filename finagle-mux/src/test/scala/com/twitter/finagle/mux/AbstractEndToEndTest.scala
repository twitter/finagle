package com.twitter.finagle.mux

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.BackupRequestFilter
import com.twitter.finagle.client.EndpointerStackClient
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.mux.lease.exp.Lessee
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.BadMessageException
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.server.ListeningStackServer
import com.twitter.finagle.service.Retries
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader
import com.twitter.util._
import java.io.PrintWriter
import java.io.StringWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger
import org.scalactic.source.Position
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.BeforeAndAfter
import org.scalatest.Tag
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractEndToEndTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience
    with BeforeAndAfter
    with AssertionsForJUnit {

  type ClientT <: EndpointerStackClient[Request, Response, ClientT]
  type ServerT <: ListeningStackServer[Request, Response, ServerT]

  def implName: String
  def skipWholeTest: Boolean = false
  def clientImpl(): ClientT
  def serverImpl(): ServerT
  def await[A](f: Awaitable[A], timeout: Duration = 5.seconds): A = Await.result(f, timeout)

  var saveBase: Dtab = Dtab.empty

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  private[this] def getName(server: ListeningServer): Name =
    Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    if (skipWholeTest) ignore(testName)(f)
    else {
      super.test(s"TLS Snooping enabled: $testName", testTags: _*) {
        toggle.flag.overrides.let(TlsSnoopingByDefault.ToggleName, 1.0) {
          liveness.sessionFailureDetector.let("none") { f }
        }
      }
      super.test(testName, testTags: _*) {
        toggle.flag.overrides.let(TlsSnoopingByDefault.ToggleName, 0.0) {
          liveness.sessionFailureDetector.let("none") { f }
        }
      }
    }
  }

  test("Shared metrics are scoped to the client and server names") {
    val sr = new InMemoryStatsReceiver

    val server = serverImpl
      .withStatsReceiver(sr)
      .withLabel("srv")
      .serve(
        "localhost:*",
        Service.const[Response](Future.value(Response(Nil, Buf.Utf8("foo"))))
      )

    val client = clientImpl
      .withStatsReceiver(sr)
      .newService(getName(server), "client")

    await(client(Request(Path.empty, Nil, Buf.Empty)), 30.seconds)

    withClue(sr.counters) {
      val clientUnscopedStatsExists = sr.counters.keySet.exists {
        case Seq("mux", _*) => true
        case _ => false
      }
      assert(!clientUnscopedStatsExists)

      val clientScopedStatsExist = sr.counters.keySet.exists {
        case Seq("client", "mux", _*) => true
        case _ => false
      }
      assert(clientScopedStatsExist)

      // Servers turn
      val serverScopedStatsExist = sr.counters.keySet.exists {
        case Seq("srv", "mux", _*) => true
        case _ => false
      }
      assert(serverScopedStatsExist)
    }

    await(server.close())
    await(client.close())
  }

  test(s"$implName: Dtab propagation") {

    val server = serverImpl.serve(
      "localhost:*",
      Service.mk[Request, Response] { _ =>
        val stringer = new StringWriter
        val printer = new PrintWriter(stringer)
        Dtab.local.print(printer)
        Future.value(Response(Nil, Buf.Utf8(stringer.toString)))
      }
    )

    val client = clientImpl.newService(getName(server), "client")

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      for (n <- 0 until 2) {
        val rsp = await(client(Request(Path.empty, Nil, Buf.Empty)), 30.seconds)
        val Buf.Utf8(str) = rsp.body
        assert(
          str.replace("\r", "") == "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
      }
    }
    await(server.close())
    await(client.close())
  }

  test(s"$implName: (no) Dtab propagation") {
    val server = serverImpl.serve(
      "localhost:*",
      Service.mk[Request, Response] { _ =>
        val bw = BufByteWriter.fixed(4)
        bw.writeIntBE(Dtab.local.size)
        Future.value(Response(Nil, bw.owned()))
      })

    val client = clientImpl.newService(getName(server), "client")

    val payload = await(client(Request.empty), 30.seconds).body
    val br = ByteReader(payload)

    assert(br.remaining == 4)
    assert(br.readIntBE() == 0)
    await(server.close())
    await(client.close())
  }

  test(s"$implName: (no) Dtab propagation with Dtab.limited") {
    val server = serverImpl.serve(
      "localhost:*",
      Service.mk[Request, Response] { _ =>
        val bw = BufByteWriter.fixed(4)
        bw.writeIntBE(Dtab.limited.size)
        Future.value(Response(Nil, bw.owned()))
      })

    val client = clientImpl.newService(getName(server), "client")

    Dtab.unwind {
      Dtab.limited ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      val payload = await(client(Request.empty), 30.seconds).body
      val br = ByteReader(payload)

      assert(br.remaining == 4)
      assert(br.readIntBE() == 0)
    }

    await(server.close())
    await(client.close())
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]): Unit = {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  test(s"$implName: trace propagation") {
    val tracer = new BufferingTracer

    var count: Int = 0
    var client: Service[Request, Response] = null

    val server = serverImpl
      .configured(param.Tracer(tracer))
      .configured(param.Label("theServer"))
      .serve(
        "localhost:*",
        new Service[Request, Response] {
          def apply(req: Request) = {
            count += 1
            if (count >= 1) Future.value(Response(Nil, req.body))
            else client(req)
          }
        })

    client = clientImpl
      .configured(param.Tracer(tracer))
      .newService(getName(server), "theClient")

    await(client(Request.empty), 30.seconds)

    eventually {
      assertAnnotationsInOrder(
        tracer.toSeq,
        Seq(
          Annotation.ServiceName("theClient"),
          Annotation.ClientSend,
          Annotation.ServiceName("theServer"),
          Annotation.ServerRecv,
          Annotation.ServerSend,
          Annotation.ClientRecv
        )
      )
    }

    await(server.close(), 30.seconds)
    await(client.close(), 30.seconds)
  }

  test(s"$implName: requeue nacks") {
    val n = new AtomicInteger(0)

    val service = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        if (n.getAndIncrement() == 0)
          Future.exception(Failure.rejected("better luck next time"))
        else
          Future.value(Response.empty)
      }
    }

    val a, b = serverImpl.serve("localhost:*", service)
    val name =
      Name.bound(
        Address(a.boundAddress.asInstanceOf[InetSocketAddress]),
        Address(b.boundAddress.asInstanceOf[InetSocketAddress])
      )

    val client = clientImpl.newService(name, "client")

    assert(n.get == 0)
    assert(await(client(Request.empty), 30.seconds).body.isEmpty)
    assert(n.get == 2)

    await(a.close(), 30.seconds)
    await(b.close(), 30.seconds)
    await(client.close(), 30.seconds)
  }

  test(s"$implName: propagate c.t.f.Failures") {
    var respondWith: Failure = null
    val service = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        Future.exception(respondWith)
      }
    }

    val server = serverImpl.serve("localhost:*", service)
    val address = getName(server)
    // Don't mask failures so we can examine which flags were propagated
    // Remove the retries module because otherwise requests will be retried until the default budget
    // is exceeded and then flagged as NonRetryable.
    val client = clientImpl
      .withStack(_.remove(Failure.role).remove(Retries.Role))
      .newService(address, "client")

    def check(f: Failure): Unit = {
      respondWith = f
      await(client(Request.empty).liftToTry) match {
        case Throw(rep: Failure) =>
          assert(rep.isFlagged(f.flags))
        case x =>
          fail(s"Expected Failure, got $x")
      }
    }

    check(Failure("Nah", FailureFlags.Rejected))
    check(Failure("Nope", FailureFlags.NonRetryable))
    check(Failure("", FailureFlags.Retryable))

    await(server.close(), 30.seconds)
    await(client.close(), 30.seconds)
  }

  test(s"$implName: gracefully reject sessions") {
    val factory = new ServiceFactory[Request, Response] {
      def apply(conn: ClientConnection): Future[Service[Request, Response]] =
        Future.exception(new Exception)

      def close(deadline: Time): Future[Unit] = Future.Done
    }

    val server = serverImpl.serve("localhost:*", factory)
    val client = clientImpl.newService(getName(server), "client")

    // This will try until it exhausts its budget. That's o.k.
    val failure = intercept[Failure] { await(client(Request.empty), 30.seconds) }

    // FailureFlags.Retryable is stripped.
    assert(!failure.isFlagged(FailureFlags.Retryable))

    await(server.close(), 30.seconds)
    await(client.close(), 30.seconds)
  }

  private[this] def nextPort(): Int = {
    val s = new Socket()
    s.setReuseAddress(true)
    try {
      s.bind(new InetSocketAddress(0))
      s.getLocalPort()
    } finally {
      s.close()
    }
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
    test(s"$implName: draining and restart") {
      val echo =
        new Service[Request, Response] {
          def apply(req: Request) = Future.value(Response(Nil, req.body))
        }
      val req = Request(Path.empty, Nil, Buf.Utf8("hello, world!"))

      // We need to reserve a port here because we're going to be
      // rebinding the server.
      val port = nextPort()
      val client = clientImpl.newService(s"localhost:$port")
      var server = serverImpl.serve(s"localhost:$port", echo)

      // Activate the client; this establishes a session.
      await(client(req))

      // This will stop listening, drain, and then close the session.
      await(server.close(), 30.seconds)

      // Thus the next request should fail at session establishment.
      intercept[Throwable] { await(client(req)) }

      // And eventually we recover.
      server = serverImpl.serve(s"localhost:$port", echo)
      eventually { await(client(req)) }

      await(server.close(), 30.seconds)
    }

  test(s"$implName: responds to lease") {
    class FakeLessor extends Lessor {
      @volatile
      var list: List[Lessee] = Nil

      def register(lessee: Lessee): Unit = {
        list ::= lessee
      }

      def unregister(lessee: Lessee): Unit = ()

      def observe(d: Duration): Unit = ()

      def observeArrival(): Unit = ()
    }
    val lessor = new FakeLessor

    val server = serverImpl
      .configured(Lessor.Param(lessor))
      .serve(
        "localhost:*",
        new Service[mux.Request, mux.Response] {
          def apply(req: Request) = ???
        })

    val sr = new InMemoryStatsReceiver

    val factory = clientImpl
      .configured(param.Stats(sr))
      .newClient(getName(server), "client")

    val fclient = factory()
    eventually { assert(fclient.isDefined) }

    val Some((_, available)) = sr.gauges.find {
      case (_ +: Seq("loadbalancer", "available"), _) => true
      case _ => false
    }

    val leaseCtr: () => Long = { () =>
      val Some((_, ctr)) = sr.counters.find {
        case (_ +: Seq("mux", "leased"), _) => true
        case _ => false
      }
      ctr
    }

    eventually { assert(available() == 1) }
    lessor.list.foreach(_.issue(Message.Tlease.MinLease))
    eventually { assert(leaseCtr() == 1) }
    eventually { assert(available() == 0) }
    lessor.list.foreach(_.issue(Message.Tlease.MaxLease))
    eventually { assert(leaseCtr() == 2) }
    eventually { assert(available() == 1) }

    Closable.sequence(await(fclient), server, factory).close()
  }

  test(s"$implName: measures payload sizes") {
    val sr = new InMemoryStatsReceiver
    val service = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(Nil, req.body.concat(req.body)))
    }
    val server = serverImpl
      .withLabel("server")
      .withStatsReceiver(sr)
      .serve("localhost:*", service)

    val client = clientImpl
      .withStatsReceiver(sr)
      .newService(getName(server), "client")

    await(client(Request(Path.empty, Nil, Buf.Utf8("." * 10))))

    eventually {
      assert(sr.stat("client", "request_payload_bytes")() == Seq(10.0f))
      assert(sr.stat("client", "response_payload_bytes")() == Seq(20.0f))
      assert(sr.stat("server", "request_payload_bytes")() == Seq(10.0f))
      assert(sr.stat("server", "response_payload_bytes")() == Seq(20.0f))
    }

    await(Closable.all(server, client).close())
  }

  test(s"$implName: correctly scopes non-mux stats") {
    val sr = new InMemoryStatsReceiver
    val service = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(Nil, req.body.concat(req.body)))
    }
    val server = serverImpl
      .withLabel("server")
      .withStatsReceiver(sr)
      .serve("localhost:*", service)

    val client = clientImpl
      .withStatsReceiver(sr)
      .newService(getName(server), "client")

    await(client(Request(Path.empty, Nil, Buf.Utf8("." * 10))))

    // Stats defined in the ChannelStatsHandler
    eventually {
      assert(sr.counter("client", "connects")() > 0)
      assert(sr.counter("server", "connects")() > 0)
    }

    await(Closable.all(server, client).close())
  }

  test(s"$implName: Default client stack will add RemoteInfo on BadMessageException") {

    val serviceName = "mux-client"
    val monitor = new Monitor {
      @volatile var exc: Throwable = _
      override def handle(exc: Throwable): Boolean = {
        this.exc = exc
        true
      }
    }

    val sr = new InMemoryStatsReceiver
    val configuredClient = clientImpl
      .withLabel(serviceName)
      .withStatsReceiver(sr)
      .withMonitor(monitor)

    val client = {
      val factory =
        ServiceFactory.const(Service.mk[Request, Response] { _ =>
          Future.exception(Failure.wrap(BadMessageException("so sad")))
        })

      val module = new Stack.Module[ServiceFactory[Request, Response]] {
        def role: Stack.Role = Stack.Role("sadface")
        def description: String = "sadface"
        def parameters: Seq[Stack.Param[_]] = Nil
        def make(
          params: Stack.Params,
          next: Stack[ServiceFactory[Request, Response]]
        ): Stack[ServiceFactory[Request, Response]] = Stack.leaf(this, factory)
      }

      val stack = configuredClient.stack ++ (module +: nilStack)
      stack
        .make(configuredClient.params + BindingFactory.Dest(Resolver.eval(s":*"))).toService
    }

    val result = await(client(Request.empty).liftToTry, 30.seconds)

    // The Monitor should have intercepted the Failure
    assert(monitor.exc.isInstanceOf[Failure])
    val failure = monitor.exc.asInstanceOf[Failure]

    // The client should have yielded a BadMessageException
    // because the Failure.module should have stripped the Failure
    assert(result.isThrow)
    assert(result.throwable.isInstanceOf[BadMessageException])
    val exc = result.throwable.asInstanceOf[BadMessageException]

    assert(failure.cause == Some(exc))

    // RemoteInfo should have been added by the client stack
    failure.getSource(Failure.Source.RemoteInfo) match {
      case Some(a: RemoteInfo.Available) =>
        assert(a.downstreamLabel == Some(serviceName))
        assert(a.downstreamAddr.isDefined)

      case other => fail(s"Unexpected remote info: $other")
    }
  }

  test("BackupRequestFilter's interrupts are propagated to the remote peer as ignorable") {
    val slow: Promise[Response] = new Promise[Response]
    slow.setInterruptHandler { case exn: Throwable => slow.setException(exn) }

    val slowService = BackupRequests.mkSlowService(slow)
    val server = serverImpl.serve("localhost:*", slowService)
    val client = clientImpl.newService(getName(server), "client")
    Time.withCurrentTimeFrozen(BackupRequests.mkRequestWithBackup(client))

    val e = intercept[ClientDiscardedRequestException] {
      await(slow, 5.seconds)
    }

    assert(e.getMessage == BackupRequestFilter.SupersededRequestFailureToString)
    assert(e.flags == (FailureFlags.Interrupted | FailureFlags.Ignorable))

    await(client.close(), 5.seconds)
    await(server.close(), 5.seconds)
  }

  test("BackupRequestFilter's interrupts are propagated multiple levels as ignorable") {
    val slow: Promise[Response] = new Promise[Response]
    slow.setInterruptHandler { case exn: Throwable => slow.setException(exn) }

    val slowService = BackupRequests.mkSlowService(slow)
    val server = serverImpl.serve("localhost:*", slowService)

    val proxy1Client = clientImpl.newService(getName(server), "proxy1Client")
    val proxy1Server = serverImpl.serve("localhost:*", proxy1Client)

    val proxy2Client = clientImpl.newService(getName(proxy1Server), "proxy2Client")
    val proxy2Server = serverImpl.serve("localhost:*", proxy2Client)

    val client = clientImpl.newService(getName(proxy2Server), "client")
    Time.withCurrentTimeFrozen(BackupRequests.mkRequestWithBackup(client))

    val e = intercept[ClientDiscardedRequestException] {
      await(slow)
    }

    assert(e.getMessage == BackupRequestFilter.SupersededRequestFailureToString)
    assert(e.flags == (FailureFlags.Interrupted | FailureFlags.Ignorable))

    await(client.close(), 5.seconds)
    await(proxy2Server.close(), 5.seconds)
    await(proxy2Client.close(), 5.seconds)
    await(proxy1Server.close(), 5.seconds)
    await(proxy1Client.close(), 5.seconds)
    await(server.close(), 5.seconds)
  }

}
