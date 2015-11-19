package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Failure, Path, Service, SimpleFilter, Status}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.mockito.invocation.InvocationOnMock
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, OneInstancePerTest, Tag}

private object TestContext {
  val testContext = new Contexts.broadcast.Key[Buf]("com.twitter.finagle.mux.MuxContext") {
    def marshal(buf: Buf) = buf
    def tryUnmarshal(buf: Buf) = Return(buf)
  }
}

private[mux] class ClientServerTest(canDispatch: Boolean)
  extends FunSuite
  with OneInstancePerTest
  with MockitoSugar
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience {
  val tracer = new BufferingTracer

  class Ctx(config: FailureDetector.Config = FailureDetector.NullConfig) {
    import Message.{encode, decode}

    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]

    val serverTransport =
      new QueueTransport(writeq=serverToClient, readq=clientToServer) {
        override def write(m: Message) = super.write(decode(encode(m)))
      }

    val clientTransport =
      new QueueTransport(writeq=clientToServer, readq=serverToClient) {
        override def write(m: Message) = super.write(decode(encode(m)))
      }

    val service = mock[Service[Request, Response]]

    val session = new ClientSession(
      clientTransport, config, "test", NullStatsReceiver)
    val client = ClientDispatcher.newRequestResponse(session)

    val nping = new AtomicInteger(0)
    val pingReq, pingRep = new Latch
    def ping() = {
      nping.incrementAndGet()
      val f = pingRep.get
      pingReq.flip()
      f
    }

    val filter = new SimpleFilter[Message, Message] {
      def apply(req: Message, service: Service[Message, Message]): Future[Message] = req match {
        case Message.Tdispatch(tag, _, _, _, _) if !canDispatch =>
          Future.value(Message.Rerr(tag, "Tdispatch not enabled"))
        case Message.Tping(tag) =>
          ping().before { Future.value(Message.Rping(tag)) }
        case req => service(req)
      }
    }

    val server = new ServerDispatcher(
      serverTransport, filter andThen Processor andThen service,
      Lessor.nil, tracer, NullStatsReceiver)
  }

  // Push a tracer for the client.
  override def test(testName: String, testTags: Tag*)(f: => Unit): Unit =
    super.test(testName, testTags:_*) {
      Trace.letTracer(tracer)(f)
    }

  def buf(b: Byte*) = Buf.ByteArray(b:_*)

  test("handle concurrent requests, handling out of order replies") {
    val ctx = new Ctx
    import ctx._

    val p1, p2, p3 = new Promise[Response]
    val reqs = (1 to 3) map { i => Request(Path.empty, buf(i.toByte)) }
    when(service(reqs(0))).thenReturn(p1)
    when(service(reqs(1))).thenReturn(p2)
    when(service(reqs(2))).thenReturn(p3)

    val f1 = client(reqs(0))
    val f2 = client(reqs(1))
    val f3 = client(reqs(2))

    for (i <- 0 to 2)
      verify(service)(reqs(i))

    for (f <- Seq(f1, f2, f3))
      assert(f.poll == None)

    val reps = Seq(10, 20, 9) map { i => Response(buf(i.toByte)) }
    p2.setValue(reps(1))
    assert(f1.poll == None)
    assert(f2.poll == Some(Return(reps(1))))
    assert(f3.poll == None)

    p1.setValue(reps(0))
    assert(f1.poll == Some(Return(reps(0))))
    assert(f3.poll == None)

    p3.setValue(reps(2))
    assert(f3.poll == Some(Return(reps(2))))
  }

  test("server responds to pings") {
    val ctx = new Ctx
    import ctx._
    for (i <- 0 until 5) {
      assert(nping.get == i)
      val pinged = session.ping()
      assert(!pinged.isDone)
      pingRep.flip()
      Await.result(pinged, 30.seconds)
      assert(pinged.isDone)
      assert(nping.get == i+1)
    }
  }

  test("server nacks new requests after draining") {
    val ctx = new Ctx
    import ctx._

    val req1 = Request(Path.empty, buf(1))
    val p1 = new Promise[Response]
    when(service(req1)).thenReturn(p1)

    val f1 = client(req1)
    verify(service)(req1)
    server.close(Time.now)
    assert(f1.poll == None)
    val req2 = Request(Path.empty, buf(2))
    client(req2).poll match {
      case Some(Throw(f: Failure)) => assert(f.isFlagged(Failure.Restartable))
      case _ => fail()
    }
    verify(service, never)(req2)

    val rep1 = Response(buf(123))
    p1.setValue(rep1)
    assert(f1.poll == Some(Return(rep1)))
  }

  test("requeueable failures transit server-to-client") {
    val ctx = new Ctx
    import ctx._

    val req1 = Request(Path.empty, buf(1))
    val p1 = new Promise[Response]
    when(service(req1)).thenReturn(Future.exception(
      Failure.rejected("come back tomorrow")))

    client(req1).poll match {
      case Some(Throw(f: Failure)) => assert(f.isFlagged(Failure.Restartable))
      case bad => fail(s"got $bad")
    }
  }

  test("handle errors") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.empty, buf(1))
    when(service(req)).thenReturn(Future.exception(new Exception("sad panda")))
    assert(client(req).poll == Some(
      Throw(ServerApplicationError("java.lang.Exception: sad panda"))))
  }

  test("propagate interrupts") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.empty, buf(1))
    val p = new Promise[Response]
    when(service(req)).thenReturn(p)
    val f = client(req)

    assert(f.poll == None)
    assert(p.isInterrupted == None)

    val exc = new Exception("sad panda")
    f.raise(exc)
    assert(p.isInterrupted == Some(
      ClientDiscardedRequestException("java.lang.Exception: sad panda")))

    assert(f.poll == Some(Throw(exc)))
  }

  test("propagate trace ids") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]]() {
        def answer(invocation: InvocationOnMock) =
          Future.value(Response(Buf.Utf8(Trace.id.toString)))
      }
    )

    val id = Trace.nextId
    val resp = Trace.letId(id) {
      client(Request(Path.empty, buf(1)))
    }
    assert(resp.poll.isDefined)
    val Buf.Utf8(respStr) = Await.result(resp).body
    assert(respStr == id.toString)
  }

  test("propagate trace flags") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) = {
          val buf = ChannelBuffers.directBuffer(8)
          buf.writeLong(Trace.id.flags.toLong)
          Future.value(Response(ChannelBufferBuf.Owned(buf)))
        }
      }
    )

    val flags = Flags().setDebug
    val id = Trace.nextId.copy(flags=flags)
    val resp = Trace.letId(id) {
      val p = client(Request(Path.empty, buf(1)))
      p
    }
    assert(resp.poll.isDefined)
    val respCb = BufChannelBuffer(Await.result(resp).body)
    assert(respCb.readableBytes == 8)
    val respFlags = Flags(respCb.readLong())
    assert(respFlags == flags)
  }

  test("failure detection") {
    val config = FailureDetector.ThresholdConfig(10.milliseconds)
    val ctx = new Ctx(config)
    import ctx._

    assert(nping.get == 1)
    assert(client.status == Status.Busy)
    pingRep.flip()
    Status.awaitOpen(client.status)

    // This is technically racy, but would require a pretty
    // pathological test environment.
    assert(client.status == Status.Open)
    eventually { assert(client.status == Status.Busy) }

    // Now begin replying.
    def loop(): Future[Unit] = {
      val f = pingReq.get
      pingRep.flip()
      f.before(loop())
    }
    loop()
    eventually {
      assert(client.status == Status.Open)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ClientServerTestNoDispatch extends ClientServerTest(false) {
  test("does not dispatch destinations") {
    val ctx = new Ctx
    import ctx._

    val withDst = Request(Path.read("/dst/name"), buf(123))
    val withoutDst = Request(Path.empty, buf(123))
    val rep = Response(buf(23))
    when(service(withoutDst)).thenReturn(Future.value(rep))
    assert(Await.result(client(withDst)) == rep)
    verify(service)(withoutDst)
  }
}

@RunWith(classOf[JUnitRunner])
class ClientServerTestDispatch extends ClientServerTest(true) {
  import TestContext._

  // Note: We test trace propagation here, too,
  // since it's a default request context.
  test("Transmits request contexts") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) =
          Future.value(Response(
            Contexts.broadcast.get(testContext)
            .getOrElse(Buf.Empty)))
      }
    )

    // No context set
    assert(Await.result(client(Request(Path.empty, Buf.Empty))).body.isEmpty)

    val f = Contexts.broadcast.let(testContext, Buf.Utf8("My context!")) {
      client(Request.empty)
    }

    assert(Await.result(f).body == Buf.Utf8("My context!"))
  }

  test("dispatches destinations") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.read("/dst/name"), buf(123))
    val rep = Response(buf(23))
    when(service(req)).thenReturn(Future.value(rep))
    assert(Await.result(client(req)) == rep)
    verify(service)(req)
  }
}
