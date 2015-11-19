package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise, Duration, Closable, Time}
import java.io.{PrintWriter, StringWriter}
import java.net.{Socket, InetSocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite
  with Eventually
  with IntegrationPatience
  with BeforeAndAfter
  with AssertionsForJUnit {

  var saveBase: Dtab = Dtab.empty

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  test("Discard request properly sent") (sessionFailureDetector.let("none") {
    @volatile var handled = false
    val p = Promise[Response]()
    p.setInterruptHandler { case t: Throwable =>
      handled = true
    }

    val svc = Service.mk[Request, Response](_ => p)

    val q0, q1 = new AsyncQueue[Message]
    val clientTrans = new QueueTransport[Message, Message](q0, q1)
    val serverTrans = new QueueTransport[Message, Message](q1, q0)

    val server = ServerDispatcher.newRequestResponse(serverTrans, svc)
    val session = new ClientSession(
      clientTrans, FailureDetector.NullConfig, "test", NullStatsReceiver)
    val client = ClientDispatcher.newRequestResponse(session)

    val f = client(Request(Path.empty, Buf.Empty))
    assert(!f.isDefined)
    assert(!p.isDefined)
    f.raise(new Exception())
    eventually { assert(handled) }
  })

  test("Dtab propagation") (sessionFailureDetector.let("none") {
    val server = Mux.serve("localhost:*", Service.mk[Request, Response] { _ =>
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      Future.value(Response(Buf.Utf8(stringer.toString)))
    })

    val client = Mux.newService(server)

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      for (n <- 0 until 2) {
        val rsp = Await.result(client(Request(Path.empty, Buf.Empty)), 30.seconds)
        val Buf.Utf8(str) = rsp.body
        assert(str == "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
      }
    }
    Await.result(server.close())
    Await.result(client.close())
  })

  test("(no) Dtab propagation") (sessionFailureDetector.let("none") {
    val server = Mux.serve("localhost:*", Service.mk[Request, Response] { _ =>
      val buf = ChannelBuffers.buffer(4)
      buf.writeInt(Dtab.local.size)
      Future.value(Response(ChannelBufferBuf.Owned(buf)))
    })

    val client = Mux.newService(server)

    val payload = Await.result(client(Request.empty), 30.seconds).body
    val cb = BufChannelBuffer(payload)

    assert(cb.readableBytes() === 4)
    assert(cb.readInt() === 0)
    Await.result(server.close())
    Await.result(client.close())
  })

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } == annos)
  }

  test("trace propagation") (sessionFailureDetector.let("none") {
    val tracer = new BufferingTracer

    var count: Int = 0
    var client: Service[Request, Response] = null

    val server = Mux.server
      .configured(param.Tracer(tracer))
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
      .configured(param.Label("theClient"))
      .newService(server)

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
  })

  test("requeue nacks") (sessionFailureDetector.let("none") {
    val n = new AtomicInteger(0)

    val service = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        if (n.getAndIncrement() == 0)
          Future.exception(Failure.rejected("better luck next time"))
        else
          Future.value(Response.empty)
      }
    }

    val a, b = Mux.serve("localhost:*", service)
    val client = Mux.newService(Name.bound(a.boundAddress, b.boundAddress), "client")

    assert(n.get == 0)
    assert(Await.result(client(Request.empty), 30.seconds).body.isEmpty)
    assert(n.get == 2)

    Await.result(a.close(), 30.seconds)
    Await.result(b.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
  })

  test("gracefully reject sessions") (sessionFailureDetector.let("none") {
    val factory = new ServiceFactory[Request, Response] {
      def apply(conn: ClientConnection): Future[Service[Request, Response]] =
        Future.exception(new Exception)

      def close(deadline: Time): Future[Unit] = Future.Done
    }

    val server = Mux.serve("localhost:*", factory)
    val client = Mux.newService(server)

    // This will try until it exhausts its budget. That's o.k.
    val failure = intercept[Failure] { Await.result(client(Request.empty)) }

    // Failure.Restartable is stripped.
    assert(!failure.isFlagged(Failure.Restartable))

    Await.result(server.close(), 30.seconds)
    Await.result(client.close(), 30.seconds)
  })

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
  test("draining and restart") (sessionFailureDetector.let("none") {
    val echo =
      new Service[Request, Response] {
        def apply(req: Request) = Future.value(Response(req.body))
      }
    val req = Request(Path.empty, Buf.Utf8("hello, world!"))

    // We need to reserve a port here because we're going to be
    // rebinding the server.
    val port = nextPort()
    val client = Mux.newService(s"localhost:$port")
    var server = Mux.serve(s"localhost:$port", echo)

    // Activate the client; this establishes a session.
    Await.result(client(req))

    // This will stop listening, drain, and then close the session.
    Await.result(server.close(), 30.seconds)

    // Thus the next request should fail at session establishment.
    intercept[Throwable] { Await.result(client(req)) }

    // And eventually we recover.
    server = Mux.serve(s"localhost:$port", echo)
    eventually { Await.result(client(req)) }

    Await.result(server.close(), 30.seconds)
  })

  test("responds to lease") (sessionFailureDetector.let("none") {
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
        .configured(Lessor.Param(lessor))
        .serve("localhost:*", new Service[mux.Request, mux.Response] {
          def apply(req: Request) = ???
        }
      )

      val sr = new InMemoryStatsReceiver

      val factory = Mux.client.configured(param.Stats(sr)).newClient(server)
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

      Closable.sequence(Await.result(fclient), server, factory).close()
    }
  })
}
