package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.{BufferingTracer, Flags, Trace}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Path, Service}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time, TimeControl}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite, Tag}

private object TestContext {
  val testContext = new Contexts.broadcast.Key[Buf] {
    val marshalId = Buf.Utf8("com.twitter.finagle.mux.MuxContext")
    def marshal(buf: Buf) = buf
    def tryUnmarshal(buf: Buf) = Return(buf)
  }
}

private[mux] class ClientServerTest(canDispatch: Boolean)
  extends FunSuite with OneInstancePerTest with MockitoSugar with AssertionsForJUnit {
  import TestContext._

  val tracer = new BufferingTracer

  val clientToServer = new AsyncQueue[ChannelBuffer]
  val serverToClient = new AsyncQueue[ChannelBuffer]
  val serverTransport =
    new QueueTransport(writeq=serverToClient, readq=clientToServer)
  val clientTransport =
    new QueueTransport(writeq=clientToServer, readq=serverToClient)
  val service = mock[Service[Ask, Response]]
  val client = new ClientDispatcher("test", clientTransport, NullStatsReceiver)
  val server = new ServerDispatcher(
    serverTransport, service, canDispatch, 
    Lessor.nil, tracer)
  
  // Push a tracer for the client.
  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    super.test(testName, testTags:_*) {
      Trace.letTracer(tracer)(f)      
      }
    }

  def buf(b: Byte*) = Buf.ByteArray(b:_*)

  test("handle concurrent requests, handling out of order replies") {
    val p1, p2, p3 = new Promise[Response]
    val reqs = (1 to 3) map { i => Ask(Path.empty, buf(i.toByte)) }
    when(service(reqs(0))).thenReturn(p1)
    when(service(reqs(1))).thenReturn(p2)
    when(service(reqs(2))).thenReturn(p3)

    val f1 = client(reqs(0))
    val f2 = client(reqs(1))
    val f3 = client(reqs(2))

    for (i <- 0 to 2)
      verify(service)(reqs(i))

    for (f <- Seq(f1, f2, f3))
      assert(f.poll === None)

    val reps = Seq(10, 20, 9) map { i => Response(buf(i.toByte)) }
    p2.setValue(reps(1))
    assert(f1.poll === None)
    assert(f2.poll === Some(Return(reps(1))))
    assert(f3.poll === None)

    p1.setValue(reps(0))
    assert(f1.poll === Some(Return(reps(0))))
    assert(f3.poll === None)

    p3.setValue(reps(2))
    assert(f3.poll === Some(Return(reps(2))))
  }

  test("server respond to pings") {
    assert(client.ping().isDefined)
  }

  test("server nacks new requests after draining") {
    val req1 = Ask(Path.empty, buf(1))
    val p1 = new Promise[Response]
    when(service(req1)).thenReturn(p1)

    val f1 = client(req1)
    verify(service)(req1)
    server.close(Time.now)
    assert(f1.poll === None)
    val req2 = Ask(Path.empty, buf(2))
    assert(client(req2).poll === Some(Throw(AskNackedException)))
    verify(service, never)(req2)

    val rep1 = Response(buf(123))
    p1.setValue(rep1)
    assert(f1.poll === Some(Return(rep1)))
  }

  test("handle errors") {
    val req = Ask(Path.empty, buf(1))
    when(service(req)).thenReturn(Future.exception(new Exception("sad panda")))
    assert(client(req).poll === Some(
      Throw(ServerApplicationError("java.lang.Exception: sad panda"))))
  }

  test("propagate interrupts") {
    val req = Ask(Path.empty, buf(1))
    val p = new Promise[Response]
    when(service(req)).thenReturn(p)
    val f = client(req)

    assert(f.poll === None)
    assert(p.isInterrupted === None)

    val exc = new Exception("sad panda")
    f.raise(exc)
    assert(p.isInterrupted === Some(
      ClientDiscardedAskException("java.lang.Exception: sad panda")))

    assert(f.poll === Some(Throw(exc)))
  }

  test("propagate trace ids") {
    when(service(any[Ask])).thenAnswer(
      new Answer[Future[Response]]() {
        def answer(invocation: InvocationOnMock) =
          Future.value(Response(Buf.Utf8(Trace.id.toString)))
      }
    )

    val id = Trace.nextId
    val resp = Trace.letId(id) {
      client(Ask(Path.empty, buf(1)))
    }
    assert(resp.poll.isDefined)
    val Buf.Utf8(respStr) = Await.result(resp).body
    assert(respStr === id.toString)
  }

  test("propagate trace flags") {
    when(service(any[Ask])).thenAnswer(
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
      val p = client(Ask(Path.empty, buf(1)))
      p
    }
    assert(resp.poll.isDefined)
    val respCb = BufChannelBuffer(Await.result(resp).body)
    assert(respCb.readableBytes === 8)
    val respFlags = Flags(respCb.readLong())
    assert(respFlags === flags)
  }
}

@RunWith(classOf[JUnitRunner])
class ClientServerTestNoDispatch extends ClientServerTest(false) {
  test("does not dispatch destinations") {
    val withDst = Ask(Path.read("/dst/name"), buf(123))
    val withoutDst = Ask(Path.empty, buf(123))
    val rep = Response(buf(23))
    when(service(withoutDst)).thenReturn(Future.value(rep))
    assert(Await.result(client(withDst)) === rep)
    verify(service)(withoutDst)
  }
}

@RunWith(classOf[JUnitRunner])
class ClientServerTestDispatch extends ClientServerTest(true) {
  import TestContext._

  // Note: We test trace propagation here, too,
  // since it's a default request context.

  test("Transmits request contexts") {
    when(service(any[Ask])).thenAnswer(
      new Answer[Future[Response]] {
        def answer(invocation: InvocationOnMock) =
          Future.value(Response(
            Contexts.broadcast.get(testContext)
            .getOrElse(Buf.Empty)))
      }
    )

    // No context set
    assert(Await.result(client(Ask(Path.empty, Buf.Empty))).body.isEmpty)

    val f = Contexts.broadcast.let(testContext, Buf.Utf8("My context!")) {
      client(Ask.empty)
    }

    assert(Await.result(f).body === Buf.Utf8("My context!"))
  }

  test("dispatches destinations") {
    val req = Ask(Path.read("/dst/name"), buf(123))
    val rep = Response(buf(23))
    when(service(req)).thenReturn(Future.value(rep))
    assert(Await.result(client(req)) === rep)
    verify(service)(req)
  }
}
