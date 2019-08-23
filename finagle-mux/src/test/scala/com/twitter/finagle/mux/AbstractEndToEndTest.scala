package com.twitter.finagle.mux

import com.twitter.conversions.PercentOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.{EndpointerStackClient, BackupRequestFilter}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor}
import com.twitter.finagle.mux.transport.{BadMessageException, Message}
import com.twitter.finagle.server.ListeningStackServer
import com.twitter.finagle.service.{Retries, ReqRep, ResponseClass, ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.MockWindowedPercentileHistogram
import com.twitter.io.{Buf, BufByteWriter, ByteReader}
import com.twitter.util._
import com.twitter.util.tunable.Tunable
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.{BeforeAndAfter, FunSuite, Tag}
import scala.language.reflectiveCalls

abstract class AbstractEndToEndTest
    extends FunSuite
    with Eventually
    with IntegrationPatience
    with BeforeAndAfter
    with AssertionsForJUnit {

  type ClientT <: EndpointerStackClient[Request, Response, ClientT]
  type ServerT <: ListeningStackServer[Request, Response, ServerT]

  def implName: String
  def clientImpl(): ClientT
  def serverImpl(): ServerT

  var saveBase: Dtab = Dtab.empty

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      liveness.sessionFailureDetector.let("none") { f }
    }
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

    val client = clientImpl.newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      for (n <- 0 until 2) {
        val rsp = Await.result(client(Request(Path.empty, Nil, Buf.Empty)), 30.seconds)
        val Buf.Utf8(str) = rsp.body
        assert(str == "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
      }
    }
    Await.result(server.close(), 5.seconds)
    Await.result(client.close(), 5.seconds)
  }

  test(s"$implName: (no) Dtab propagation") {
    val server = serverImpl.serve("localhost:*", Service.mk[Request, Response] { _ =>
      val bw = BufByteWriter.fixed(4)
      bw.writeIntBE(Dtab.local.size)
      Future.value(Response(Nil, bw.owned()))
    })

    val client = clientImpl.newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val payload = Await.result(client(Request.empty), 30.seconds).body
    val br = ByteReader(payload)

    assert(br.remaining == 4)
    assert(br.readIntBE() == 0)
    Await.result(server.close(), 5.seconds)
    Await.result(client.close(), 5.seconds)
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
      .serve("localhost:*", new Service[Request, Response] {
        def apply(req: Request) = {
          count += 1
          if (count >= 1) Future.value(Response(Nil, req.body))
          else client(req)
        }
      })

    client = clientImpl
      .configured(param.Tracer(tracer))
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "theClient"
      )

    Await.result(client(Request.empty), 30.seconds)

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

    Await.result(server.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
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
    assert(Await.result(client(Request.empty), 30.seconds).body.isEmpty)
    assert(n.get == 2)

    Await.result(a.close(), 30.seconds)
    Await.result(b.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
  }

  test(s"$implName: propagate c.t.f.Failures") {
    var respondWith: Failure = null
    val service = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        Future.exception(respondWith)
      }
    }

    val server = serverImpl.serve("localhost:*", service)
    val address = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    // Don't mask failures so we can examine which flags were propagated
    // Remove the retries module because otherwise requests will be retried until the default budget
    // is exceeded and then flagged as NonRetryable.
    val client = clientImpl
      .withStack(_.remove(Failure.role).remove(Retries.Role))
      .newService(address, "client")

    def check(f: Failure): Unit = {
      respondWith = f
      Await.result(client(Request.empty).liftToTry, 5.seconds) match {
        case Throw(rep: Failure) =>
          assert(rep.isFlagged(f.flags))
        case x =>
          fail(s"Expected Failure, got $x")
      }
    }

    check(Failure("Nah", FailureFlags.Rejected))
    check(Failure("Nope", FailureFlags.NonRetryable))
    check(Failure("", FailureFlags.Retryable))

    Await.result(server.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
  }

  test(s"$implName: gracefully reject sessions") {
    val factory = new ServiceFactory[Request, Response] {
      def apply(conn: ClientConnection): Future[Service[Request, Response]] =
        Future.exception(new Exception)

      def close(deadline: Time): Future[Unit] = Future.Done
    }

    val server = serverImpl.serve("localhost:*", factory)
    val client = clientImpl.newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    // This will try until it exhausts its budget. That's o.k.
    val failure = intercept[Failure] { Await.result(client(Request.empty), 30.seconds) }

    // FailureFlags.Retryable is stripped.
    assert(!failure.isFlagged(FailureFlags.Retryable))

    Await.result(server.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
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
      Await.result(client(req), 5.seconds)

      // This will stop listening, drain, and then close the session.
      Await.result(server.close(), 30.seconds)

      // Thus the next request should fail at session establishment.
      intercept[Throwable] { Await.result(client(req), 5.seconds) }

      // And eventually we recover.
      server = serverImpl.serve(s"localhost:$port", echo)
      eventually { Await.result(client(req), 5.seconds) }

      Await.result(server.close(), 30.seconds)
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
      .serve("localhost:*", new Service[mux.Request, mux.Response] {
        def apply(req: Request) = ???
      })

    val sr = new InMemoryStatsReceiver

    val factory = clientImpl
      .configured(param.Stats(sr))
      .newClient(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

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

    Closable.sequence(Await.result(fclient, 5.seconds), server, factory).close()
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
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    Await.ready(client(Request(Path.empty, Nil, Buf.Utf8("." * 10))), 5.seconds)

    eventually {
      assert(sr.stat("client", "request_payload_bytes")() == Seq(10.0f))
      assert(sr.stat("client", "response_payload_bytes")() == Seq(20.0f))
      assert(sr.stat("server", "request_payload_bytes")() == Seq(10.0f))
      assert(sr.stat("server", "response_payload_bytes")() == Seq(20.0f))
    }

    Await.ready(Closable.all(server, client).close(), 5.seconds)
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
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    Await.ready(client(Request(Path.empty, Nil, Buf.Utf8("." * 10))), 5.seconds)

    // Stats defined in the ChannelStatsHandler
    eventually {
      assert(sr.counter("client", "connects")() > 0)
      assert(sr.counter("server", "connects")() > 0)
    }

    Await.ready(Closable.all(server, client).close(), 5.seconds)
  }

  test(s"$implName: Default client stack will add RemoteInfo on BadMessageException") {

    class Server {
      private lazy val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      private lazy val server = new ServerSocket()
      @volatile private var client: Socket = _

      private[this] def swallowMessage(): Unit = {
        val is = client.getInputStream
        val sizeField = (3 to 0 by -1).foldLeft(0) { (acc: Int, i: Int) =>
          acc | (is.read().toByte << i)
        }
        // swallow sizeField bytes
        (0 until sizeField).foreach(_ => is.read())
      }

      private[this] def swallowAndWrite(data: Array[Byte]): Unit = {
        swallowMessage()
        val os = client.getOutputStream
        data.foreach(os.write(_))
        os.flush()
      }

      private val serverThread = new Thread(new Runnable {
        override def run(): Unit = {
          client = server.accept()
          // Length of 4 bytes, header of 0x00 00 00 04 (illegal: message type 0x00)
          val rerr = Message.encode(Message.Rerr(1, "didn't work!"))
          val badMessage = Array[Byte](0, 0, 0, 4, 0, 0, 0, 4)

          val lengthField = {
            val len = new Array[Byte](4)
            val bb = ByteBuffer.wrap(len)
            bb.putInt(rerr.length)
            len
          }

          // write the message twice: once for handshaking and once for the failure
          swallowAndWrite(lengthField ++ Buf.ByteArray.Shared.extract(rerr))
          swallowAndWrite(badMessage)
        }
      })

      def port = server.getLocalPort

      def start(): Unit = {
        server.bind(address)
        serverThread.start()
      }

      def close(): Unit = {
        serverThread.join(30.seconds.inMillis)
        Option(client).foreach(_.close())
        server.close()
      }
    }

    val server = new Server
    val serviceName = "mux-client"

    val monitor = new Monitor {
      @volatile var exc: Throwable = _
      override def handle(exc: Throwable): Boolean = {
        this.exc = exc
        true
      }
    }

    try {
      server.start()
      val sr = new InMemoryStatsReceiver
      val client = clientImpl
        .withLabel(serviceName)
        .withStatsReceiver(sr)
        .withMonitor(monitor)
        .newService(s"${InetAddress.getLoopbackAddress.getHostAddress}:${server.port}")

      val result = Await.result(client(Request.empty).liftToTry, 5.seconds)
      server.close()

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
    } finally {
      server.close()
    }
  }

  test("BackupRequestFilter's interrupts are propagated to the remote peer as ignorable") {
    val slow: Promise[Response] = new Promise[Response]
    val first = new AtomicBoolean(true)
    slow.setInterruptHandler {
      case exn: Throwable =>
        slow.setException(exn)
    }
    val server = serverImpl.serve(
      "localhost:*",
      Service.mk[Request, Response] { _ =>
        if (first.compareAndSet(true, false)) slow
        else {
          Future.value(Response.empty)
        }
      }
    )

    val client = clientImpl.newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    val tunable: Tunable.Mutable[Double] = Tunable.mutable[Double]("tunable", 1.percent)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Return(_)) => ResponseClass.Success
    }
    val retryBudget = RetryBudget.Infinite
    Time.withCurrentTimeFrozen { ctl =>
      val timer = new MockTimer()

      val wp = new MockWindowedPercentileHistogram(timer)
      wp.add(1.second.inMillis.toInt)

      val brf = new BackupRequestFilter[Request, Response](
        tunable,
        true,
        classifier,
        (_, _) => retryBudget,
        retryBudget,
        Stopwatch.timeMillis,
        NullStatsReceiver,
        timer,
        () => wp
      )
      ctl.advance(3.seconds)
      timer.tick()

      val filtered = brf.andThen(client)

      val f = filtered(Request.empty)

      ctl.advance(2.seconds)
      timer.tick()

      Await.result(f, 5.seconds)
    }

    val e = intercept[ClientDiscardedRequestException] {
      Await.result(slow, 5.seconds)
    }

    assert(e.getMessage == BackupRequestFilter.SupersededRequestFailureToString)
    assert(e.flags == (FailureFlags.Interrupted | FailureFlags.Ignorable))
  }
}
