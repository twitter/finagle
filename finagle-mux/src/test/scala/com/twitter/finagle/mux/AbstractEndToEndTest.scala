package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.Mux.param.MuxImpl
import com.twitter.finagle._
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor}
import com.twitter.finagle.mux.transport.{BadMessageException, Message}
import com.twitter.finagle.service.Retries
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing._
import com.twitter.io.{Buf, ByteReader, ByteWriter}
import com.twitter.util._
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.{BeforeAndAfter, FunSuite, Tag}
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
abstract class AbstractEndToEndTest extends FunSuite
  with Eventually
  with IntegrationPatience
  with BeforeAndAfter
  with AssertionsForJUnit {

  def implName: String
  def clientImpl(): MuxImpl
  def serverImpl(): MuxImpl

  var saveBase: Dtab = Dtab.empty

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position) {
    super.test(testName, testTags:_*) {
      liveness.sessionFailureDetector.let("none") { f }
    }
  }


  test(s"$implName: Dtab propagation") {

    val server = Mux.server.configured(serverImpl).serve("localhost:*", Service.mk[Request, Response] { _ =>
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      Future.value(Response(Buf.Utf8(stringer.toString)))
    })

    val client = Mux.client.configured(clientImpl).newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      for (n <- 0 until 2) {
        val rsp = Await.result(client(Request(Path.empty, Buf.Empty)), 30.seconds)
        val Buf.Utf8(str) = rsp.body
        assert(str == "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
      }
    }
    Await.result(server.close(), 5.seconds)
    Await.result(client.close(), 5.seconds)
  }

  test(s"$implName: (no) Dtab propagation") {
    val server = Mux.server.configured(serverImpl).serve("localhost:*", Service.mk[Request, Response] { _ =>
      val bw = ByteWriter.fixed(4)
      bw.writeIntBE(Dtab.local.size)
      Future.value(Response(bw.owned()))
    })

    val client = Mux.client.configured(clientImpl).newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val payload = Await.result(client(Request.empty), 30.seconds).body
    val br = ByteReader(payload)

    assert(br.remaining == 4)
    assert(br.readIntBE() == 0)
    Await.result(server.close(), 5.seconds)
    Await.result(client.close(), 5.seconds)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  test(s"$implName: trace propagation") {
    val tracer = new BufferingTracer

    var count: Int = 0
    var client: Service[Request, Response] = null

    val server = Mux.server
      .configured(param.Tracer(tracer))
      .configured(serverImpl)
      .configured(param.Label("theServer"))
      .serve("localhost:*", new Service[Request, Response] {
        def apply(req: Request) = {
          count += 1
          if (count >= 1) Future.value(Response(req.body))
          else client(req)
        }
      })

    client = Mux.client
      .configured(param.Tracer(tracer))
      .configured(clientImpl)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "theClient")

    Await.result(client(Request.empty), 30.seconds)

    assertAnnotationsInOrder(tracer.toSeq, Seq(
      Annotation.ServiceName("theClient"),
      Annotation.ClientSend(),
      Annotation.BinaryAnnotation("clnt/mux/enabled", true),
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.BinaryAnnotation("srv/mux/enabled", true),
      Annotation.ServerSend(),
      Annotation.ClientRecv()
    ))

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

    val a, b = Mux.server.configured(serverImpl).serve("localhost:*", service)
    val name =
      Name.bound(
        Address(a.boundAddress.asInstanceOf[InetSocketAddress]),
        Address(b.boundAddress.asInstanceOf[InetSocketAddress])
      )

    val client = Mux.client.configured(clientImpl).configured(clientImpl).newService(name, "client")

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

    val server = Mux.serve("localhost:*", service)
    val address = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    // Don't mask failures so we can examine which flags were propagated
    // Remove the retries module because otherwise requests will be retried until the default budget
    // is exceeded and then flagged as NonRetryable.
    val client = Mux.client.transformed(_.remove(Failure.role).remove(Retries.Role))
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

    check(Failure("Nah", Failure.Rejected))
    check(Failure("Nope", Failure.NonRetryable))
    check(Failure("", Failure.Restartable))

    Await.result(server.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
  }

  test(s"$implName: gracefully reject sessions") {
    val factory = new ServiceFactory[Request, Response] {
      def apply(conn: ClientConnection): Future[Service[Request, Response]] =
        Future.exception(new Exception)

      def close(deadline: Time): Future[Unit] = Future.Done
    }

    val server = Mux.server.configured(serverImpl).serve("localhost:*", factory)
    val client = Mux.client.configured(clientImpl).newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    // This will try until it exhausts its budget. That's o.k.
    val failure = intercept[Failure] { Await.result(client(Request.empty), 30.seconds) }

    // Failure.Restartable is stripped.
    assert(!failure.isFlagged(Failure.Restartable))

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

  // This is marked FLAKY because it allocates a nonephemeral port;
  // this is unfortunately required for this type of testing (since we're
  // interested in completely shutting down, and then restarting a
  // server on the same port).
  //
  // Note also that, in the case of a single endpoint, the loadbalancer's
  // fallback behavior circumvents status propagation bugs. This is
  // because, in the event that all endpoints are down, the load balancer
  // reverts its down list, and attempts to establish a session regardless
  // of reported status.
  //
  // The following script patches up the load balancer to avoid this
  // behavior.
  /*
ed - ../../../../../../../../finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/HeapBalancer.scala <<EOF
246a
      if (n == null)
        return Future.exception(emptyException)
.
216c
    if (n.load >= 0) null
    else if (n.factory.status == Status.Open) n
    else {
.
201c
      } else if (n.factory.status == Status.Open) {  // revived node
.
132c
    nodes.count(_.factory.status == Status.Open)
.
w
EOF
  */
  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined)
  test(s"$implName: draining and restart") {
    val echo =
      new Service[Request, Response] {
        def apply(req: Request) = Future.value(Response(req.body))
      }
    val req = Request(Path.empty, Buf.Utf8("hello, world!"))

    // We need to reserve a port here because we're going to be
    // rebinding the server.
    val port = nextPort()
    val client = Mux.client.configured(clientImpl).newService(s"localhost:$port")
    var server = Mux.server.configured(serverImpl).serve(s"localhost:$port", echo)

    // Activate the client; this establishes a session.
    Await.result(client(req), 5.seconds)

    // This will stop listening, drain, and then close the session.
    Await.result(server.close(), 30.seconds)

    // Thus the next request should fail at session establishment.
    intercept[Throwable] { Await.result(client(req), 5.seconds) }

    // And eventually we recover.
    server = Mux.server.configured(serverImpl).serve(s"localhost:$port", echo)
    eventually { Await.result(client(req), 5.seconds) }

    Await.result(server.close(), 30.seconds)
  }

  test(s"$implName: responds to lease") {
    Time.withCurrentTimeFrozen { ctl =>
      class FakeLessor extends Lessor {
        var list: List[Lessee] = Nil

        def register(lessee: Lessee): Unit = {
          list ::= lessee
        }

        def unregister(lessee: Lessee): Unit = ()

        def observe(d: Duration): Unit = ()

        def observeArrival(): Unit = ()
      }
      val lessor = new FakeLessor

      val server = Mux.server
        .configured(serverImpl)
        .configured(Lessor.Param(lessor))
        .serve("localhost:*", new Service[mux.Request, mux.Response] {
          def apply(req: Request) = ???
        }
      )

      val sr = new InMemoryStatsReceiver

      val factory = Mux.client.configured(clientImpl).configured(param.Stats(sr)).newClient(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

      val fclient = factory()
      eventually { assert(fclient.isDefined) }

      val Some((_, available)) = sr.gauges.find {
        case (_ +: Seq("loadbalancer", "available"), value) => true
        case _ => false
      }

      val Some((_, leaseDuration)) = sr.gauges.find {
        case (_ +: Seq("mux", "current_lease_ms"), value) => true
        case _ => false
      }

      val leaseCtr: () => Int = { () =>
        val Some((_, ctr)) = sr.counters.find {
          case (_ +: Seq("mux", "leased"), value) => true
          case _ => false
        }
        ctr
      }
      def format(duration: Duration): Float = duration.inMilliseconds.toFloat

      eventually { assert(leaseDuration() == format(Time.Top - Time.now)) }
      eventually { assert(available() == 1) }
      lessor.list.foreach(_.issue(Message.Tlease.MinLease))
      eventually { assert(leaseCtr() == 1) }
      ctl.advance(2.seconds) // must advance time to re-lease and expire
      eventually { assert(leaseDuration() == format(Message.Tlease.MinLease - 2.seconds)) }
      eventually { assert(available() == 0) }
      lessor.list.foreach(_.issue(Message.Tlease.MaxLease))
      eventually { assert(leaseCtr() == 2) }
      eventually { assert(leaseDuration() == format(Message.Tlease.MaxLease)) }
      eventually { assert(available() == 1) }

      Closable.sequence(Await.result(fclient, 5.seconds), server, factory).close()
    }
  }

  test(s"$implName: measures payload sizes") {
    val sr = new InMemoryStatsReceiver
    val service = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(req.body.concat(req.body)))
    }
    val server = Mux.server
      .configured(serverImpl)
      .withLabel("server")
      .withStatsReceiver(sr)
      .serve("localhost:*", service)

    val client = Mux.client
      .configured(clientImpl)
      .withStatsReceiver(sr)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    Await.ready(client(Request(Path.empty, Buf.Utf8("." * 10))), 5.seconds)

    assert(sr.stat("client", "request_payload_bytes")() == Seq(10.0f))
    assert(sr.stat("client", "response_payload_bytes")() == Seq(20.0f))
    assert(sr.stat("server", "request_payload_bytes")() == Seq(10.0f))
    assert(sr.stat("server", "response_payload_bytes")() == Seq(20.0f))

    Await.ready(Closable.all(server, client).close(), 5.seconds)
  }

  test(s"$implName: Default client stack will add RemoteInfo on BadMessageException") {

    class Server {
      private lazy val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      private lazy val server = new ServerSocket()
      @volatile private var client: Socket = _

      private val serverThread = new Thread(new Runnable {
        override def run(): Unit = {
          client = server.accept()
          // Length of 4 bytes, header of 0x00 00 00 04 (illegal: message type 0x00)
          val message = Array[Byte](0, 0, 0, 4, 0, 0, 0, 4)

          // write the message twice: once for handshaking and once for the failure
          client.getOutputStream.write(message ++ message)
          client.getOutputStream.flush()
        }
      })

      def port = server.getLocalPort

      def start(): Unit = {
        server.bind(address)
        serverThread.start()
      }

      def close(): Unit = {
        serverThread.join()
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
      val client = Mux.client
        .configured(clientImpl)
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
          assert(a.downstreamId == Some(ClientId(serviceName)))
          assert(a.downstreamAddr.isDefined)

        case other => fail(s"Unexpected remote info: $other")
      }
    } finally {
      server.close()
    }
  }
}
