package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.BackupRequestFilter
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.pushsession.FragmentDecoder
import com.twitter.finagle.mux.pushsession.FragmentingMessageWriter
import com.twitter.finagle.mux.pushsession.MuxClientSession
import com.twitter.finagle.mux.pushsession.MuxServerSession
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Matchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalactic.source.Position
import org.scalatest.OneInstancePerTest
import org.scalatest.Tag
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatestplus.mockito.MockitoSugar

private object TestContext {
  val testContext = new Contexts.broadcast.Key[Buf]("com.twitter.finagle.mux.MuxContext") {
    def marshal(buf: Buf) = buf
    def tryUnmarshal(buf: Buf) = Return(buf)
  }
}

private[mux] abstract class ClientServerTest
    extends AnyFunSuite
    with OneInstancePerTest
    with MockitoSugar
    with AssertionsForJUnit
    with Eventually
    with IntegrationPatience {

  def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  def canDispatch: Boolean

  val tracer = new BufferingTracer

  class Ctx(config: FailureDetector.Config = FailureDetector.NullConfig) {

    val toServerQueue = new AsyncQueue[Buf]
    val toClientQueue = new AsyncQueue[Buf]

    private val clientHandle = new QueueChannelHandle[ByteReader, Buf](toServerQueue)
    private val serverHandle = new QueueChannelHandle[ByteReader, Buf](toClientQueue)

    val pingSends = new AtomicInteger(0)
    val pingReceives = new AtomicInteger(0)

    { // launch the read loops for each queue
      def loop(source: AsyncQueue[Buf], dest: QueueChannelHandle[ByteReader, _]): Unit = {
        source.poll().respond {
          case Return(m) =>
            val decoded = Message.decode(m)

            if (decoded.typ == Message.Types.Tping) {
              assert(source eq toServerQueue)
              pingSends.incrementAndGet()
            } else if (decoded.typ == Message.Types.Rping) {
              assert(source eq toClientQueue)
              pingReceives.incrementAndGet()
            }

            if (!canDispatch && decoded.typ == Message.Types.Tdispatch) {
              assert(source eq toServerQueue)
              assert(dest eq serverHandle)
              val err = Message.Rerr(decoded.tag, "Tdispatch not enabled")
              toClientQueue.offer(Message.encode(err))
            } else {
              dest.sessionReceive(ByteReader(m))
            }
            loop(source, dest)

          case Throw(_: ChannelClosedException) =>
            dest.failHandle(Return.Unit)

          case Throw(cause) =>
            dest.failHandle(Throw(cause))
        }
      }

      loop(toServerQueue, serverHandle)
      loop(toClientQueue, clientHandle)
    }

    val service = mock[Service[Request, Response]]
    when(service.close(any())).thenReturn(Future.Done)

    val clientSession: MuxClientSession = {
      val session = new MuxClientSession(
        handle = clientHandle,
        h_decoder = new FragmentDecoder(NullStatsReceiver),
        h_messageWriter =
          new FragmentingMessageWriter(clientHandle, Int.MaxValue, NullStatsReceiver),
        detectorConfig = config,
        name = "test",
        statsReceiver = NullStatsReceiver)
      // Register ourselves
      clientHandle.serialExecutor.execute(new Runnable {
        def run(): Unit = clientHandle.registerSession(session)
      })
      session
    }

    val server: Closable = {
      val session = new MuxServerSession(
        params = Mux.server.params,
        h_decoder = new FragmentDecoder(NullStatsReceiver),
        h_messageWriter =
          new FragmentingMessageWriter(serverHandle, Int.MaxValue, NullStatsReceiver),
        handle = serverHandle,
        service = service
      )
      serverHandle.serialExecutor.execute(new Runnable {
        def run(): Unit = serverHandle.registerSession(session)
      })
      session
    }

    val client: Service[Request, Response] = await(clientSession.asService)
  }

  // Push a tracer for the client.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit =
    super.test(testName, testTags: _*) {
      Trace.letTracer(tracer)(f)
    }

  def buf(b: Byte*) = Buf.ByteArray.Owned(b.toArray)

  test("handle concurrent requests, handling out of order replies") {
    val ctx = new Ctx
    import ctx._

    val p1, p2, p3 = new Promise[Response]
    val reqs = (1 to 3) map { i => Request(Path.empty, Nil, buf(i.toByte)) }
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

    val reps = Seq(10, 20, 9) map { i => Response(Nil, buf(i.toByte)) }
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
      assert(pingSends.get == i)
      assert(pingReceives.get == i)
      val pinged = clientSession.ping()
      await(pinged)
      assert(await(pinged.liftToTry) == Return.Unit)
      assert(pingSends.get == i + 1)
      assert(pingReceives.get == i + 1)
    }
  }

  test("server nacks new requests after draining") {
    val ctx = new Ctx
    import ctx._

    val req1 = Request(Path.empty, Nil, buf(1))
    val p1 = new Promise[Response]
    when(service(req1)).thenReturn(p1)

    val f1 = client(req1)
    verify(service)(req1)
    server.close(Time.Top)
    assert(f1.poll == None)
    val req2 = Request(Path.empty, Nil, buf(2))
    client(req2).poll match {
      case Some(Throw(f: Failure)) => assert(f.isFlagged(FailureFlags.Retryable))
      case _ => fail()
    }
    verify(service, never)(req2)

    val rep1 = Response(Nil, buf(123))
    p1.setValue(rep1)
    assert(await(f1) == rep1)
  }

  test("requeueable failures transit server-to-client") {
    val ctx = new Ctx
    import ctx._

    val req1 = Request(Path.empty, Nil, buf(1))
    val p1 = new Promise[Response]
    when(service(req1)).thenReturn(Future.exception(Failure.rejected("come back tomorrow")))

    client(req1).poll match {
      case Some(Throw(f: Failure)) => assert(f.isFlagged(FailureFlags.Retryable))
      case bad => fail(s"got $bad")
    }
  }

  test("handle errors") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.empty, Nil, buf(1))
    when(service(req)).thenReturn(Future.exception(new Exception("sad panda")))
    assert(
      client(req).poll == Some(Throw(ServerApplicationError("java.lang.Exception: sad panda")))
    )
  }

  test("propagate interrupts") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.empty, Nil, buf(1))
    val p = new Promise[Response]
    when(service(req)).thenReturn(p)
    val f = client(req)

    assert(f.poll == None)
    assert(p.isInterrupted == None)

    val exc = new Exception("sad panda")
    f.raise(exc)
    val e = intercept[ClientDiscardedRequestException] {
      throw p.isInterrupted.get
    }
    assert(e.flags == FailureFlags.Interrupted)
    assert(e.getMessage == "java.lang.Exception: sad panda")
    assert(f.poll == Some(Throw(exc)))
  }

  test("propagate interrupts with the right flags") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.empty, Nil, buf(1))
    val p = new Promise[Response]
    when(service(req)).thenReturn(p)
    val f = client(req)

    assert(f.poll == None)
    assert(p.isInterrupted == None)

    val exc = Failure.ignorable(BackupRequestFilter.SupersededRequestFailureWhy)
    f.raise(exc)
    val e = intercept[ClientDiscardedRequestException] {
      throw p.isInterrupted.get
    }
    assert(e.flags == (FailureFlags.Interrupted | FailureFlags.Ignorable))
    assert(f.poll == Some(Throw(exc)))
  }

  test("propagate trace ids") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]]() {
        def answer(invocation: InvocationOnMock) =
          Future.value(Response(Nil, Buf.Utf8(Trace.id.toString)))
      }
    )

    val id = Trace.nextId
    val resp = Trace.letId(id) {
      client(Request(Path.empty, Nil, buf(1)))
    }
    assert(resp.poll.isDefined)
    val Buf.Utf8(respStr) = await(resp).body
    assert(respStr == id.toString)
  }

  test("propagate trace flags") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) = {
          val bw = BufByteWriter.fixed(8)
          bw.writeLongBE(Trace.id.flags.toLong)
          Future.value(Response(Nil, bw.owned()))
        }
      }
    )

    val flags = Flags().setDebug
    val id = Trace.nextId.copy(flags = flags)
    val resp = Trace.letId(id) {
      val p = client(Request(Path.empty, Nil, buf(1)))
      p
    }
    assert(resp.poll.isDefined)
    val respBr = ByteReader(await(resp).body)
    assert(respBr.remaining == 8)
    val respFlags = Flags(respBr.readLongBE())
    assert(respFlags == flags)
  }
}

class ClientServerTestNoDispatch extends ClientServerTest {
  val canDispatch = false

  test("does not dispatch destinations") {
    val ctx = new Ctx
    import ctx._

    val withDst = Request(Path.read("/dst/name"), Nil, buf(123))
    val withoutDst = Request(Path.empty, Nil, buf(123))
    val rep = Response(Nil, buf(23))
    when(service(withoutDst)).thenReturn(Future.value(rep))
    assert(await(client(withDst)) == rep)
    verify(service)(withoutDst)
  }
}

class ClientServerTestDispatch extends ClientServerTest {
  val canDispatch = true

  import TestContext._

  // Note: We test trace propagation here, too,
  // since it's a default request context.
  test("Transmits request contexts") {
    val ctx = new Ctx
    import ctx._

    when(service(any[Request])).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) =
          Future.value(
            Response(
              Nil,
              Contexts.broadcast
                .get(testContext)
                .getOrElse(Buf.Empty)
            )
          )
      }
    )

    // No context set
    assert(await(client(Request(Path.empty, Nil, Buf.Empty))).body.isEmpty)

    val f = Contexts.broadcast.let(testContext, Buf.Utf8("My context!")) {
      client(Request.empty)
    }

    assert(await(f).body == Buf.Utf8("My context!"))
  }

  test("dispatches destinations") {
    val ctx = new Ctx
    import ctx._

    val req = Request(Path.read("/dst/name"), Nil, buf(123))
    val rep = Response(Nil, buf(23))
    when(service(req)).thenReturn(Future.value(rep))
    assert(await(client(req)) == rep)
    verify(service)(req)
  }

  test("propagate explicit request contexts") {
    val ctx = new Ctx
    import ctx._

    val ctxts = Seq((Buf.Utf8("HELLO"), Buf.Utf8("WORLD")))
    val request = Request(Path.empty, ctxts, Buf.Empty)

    when(service(request)).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) = {
          Future.value(Response(request.contexts, Buf.Empty))
        }
      }
    )

    val response = await(client(request))
    assert(response.contexts.nonEmpty)
    assert(response.contexts == ctxts)
  }
}
