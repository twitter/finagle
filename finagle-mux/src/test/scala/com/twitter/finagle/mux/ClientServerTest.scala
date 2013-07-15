package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Annotation, BufferingTracer, Flags, Record, SpanId, Trace, TraceId}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite}

@RunWith(classOf[JUnitRunner])
class ClientServerTest extends FunSuite with OneInstancePerTest with MockitoSugar {
  def buf(b: Byte*) = ChannelBuffers.wrappedBuffer(Array[Byte](b:_*))

  val tracer = new BufferingTracer
  Trace.pushTracer(tracer)
  val clientToServer = new AsyncQueue[ChannelBuffer]
  val serverToClient = new AsyncQueue[ChannelBuffer]
  val serverTransport = new QueueTransport(writeq=serverToClient, readq=clientToServer)
  val clientTransport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
  val service = mock[Service[ChannelBuffer, ChannelBuffer]]
  val client = new ClientDispatcher(clientTransport)
  val server = new ServerDispatcher(serverTransport, service)

  test("end-to-end with tracing: client-to-service") {
    val p = new Promise[ChannelBuffer]
    when(service(buf(1))).thenReturn(p)

    verify(service, never())(any[ChannelBuffer])
    val id = TraceId(Some(SpanId(1)), Some(SpanId(2)), SpanId(3), None)
    val f = Trace.unwind {
      Trace.setId(id)
      client(buf(1))
    }
    verify(service)(buf(1))
    assert(f.poll === None)
    p.setValue(buf(2))
    assert(f.poll === Some(Return(buf(2))))

    val ia = new InetSocketAddress(0)
    val recs = tracer.toSeq.sortBy(_.timestamp)
    assert(recs match {
      case Seq(
        Record(`id`, _, Annotation.ClientSend(), None),
        Record(`id`, _, Annotation.ServerRecv(), None),
        Record(`id`, _, Annotation.ServerSend(), None),
        Record(`id`, _, Annotation.ClientRecv(), None)) => true
      case _ => false
    })
  }

  test("handle concurrent requests, handling out of order replies") {
    val p1, p2, p3 = new Promise[ChannelBuffer]
    when(service(buf(1))).thenReturn(p1)
    when(service(buf(2))).thenReturn(p2)
    when(service(buf(3))).thenReturn(p3)

    val f1 = client(buf(1))
    val f2 = client(buf(2))
    val f3 = client(buf(3))

    for (i <- 1 to 3)
      verify(service)(buf(i.toByte))

    for (f <- Seq(f1, f2, f3))
      assert(f.poll === None)

    p2.setValue(buf(20))
    assert(f1.poll === None)
    assert(f2.poll === Some(Return(buf(20))))
    assert(f3.poll === None)

    p1.setValue(buf(10))
    assert(f1.poll === Some(Return(buf(10))))
    assert(f3.poll === None)

    p3.setValue(buf(9))
    assert(f3.poll === Some(Return(buf(9))))
  }

  test("server respond to pings") {
    assert(client.ping().isDefined)
  }

  test("server nacks new requests after draining") {
    val p1 = new Promise[ChannelBuffer]
    when(service(buf(1))).thenReturn(p1)

    val f1 = client(buf(1))
    verify(service)(buf(1))
    server.close(Time.now)
    assert(f1.poll === None)
    assert(client(buf(2)).poll === Some(Throw(RequestNackedException)))
    verify(service, never)(buf(2))

    p1.setValue(buf(123))
    assert(f1.poll === Some(Return(buf(123))))
  }

  test("handle errors") {
    when(service(buf(1))).thenReturn(Future.exception(new Exception("sad panda")))
    assert(client(buf(1)).poll === Some(
      Throw(ServerApplicationError("java.lang.Exception: sad panda"))))
  }

  test("propagate interrupts") {
    val p = new Promise[ChannelBuffer]
    when(service(buf(1))).thenReturn(p)
    val f = client(buf(1))

    assert(f.poll === None)
    assert(p.isInterrupted === None)

    val exc = new Exception("sad panda")
    f.raise(exc)
    assert(p.isInterrupted === Some(
      ClientDiscardedRequestException("java.lang.Exception: sad panda")))

    assert(f.poll === Some(Throw(exc)))
  }

  test("propagate trace ids") {
    when(service(any[ChannelBuffer])).thenAnswer(new Answer[Future[ChannelBuffer]]() {
      def answer(invocation: InvocationOnMock) = {
        val traceId = ChannelBuffers.wrappedBuffer(
          Trace.id.toString.getBytes(CharsetUtil.UTF_8))
        Future.value(traceId)
      }
    })


    val id = Trace.nextId
    val resp = Trace.unwind {
      Trace.setId(id)
      client(buf(1))
    }
    assert(resp.poll.isDefined)
    val respBuf = Await.result(resp)
    val respArr = new Array[Byte](respBuf.readableBytes)
    respBuf.readBytes(respArr)
    val respStr = new String(respArr, CharsetUtil.UTF_8)
    assert(respStr === id.toString)
  }

  test("propagate trace flags") {
    when(service(any[ChannelBuffer])).thenAnswer(new Answer[Future[ChannelBuffer]] {
      def answer(invocation: InvocationOnMock) = {
        val buf = ChannelBuffers.directBuffer(8)
        buf.writeLong(Trace.id.flags.toLong)
        Future.value(buf)
      }
    })

    val flags = Flags().setDebug
    val id = Trace.nextId.copy(flags=flags)
    val resp = Trace.unwind {
      Trace.setId(id)
      val p = client(buf(1))
      p
    }
    assert(resp.poll.isDefined)
    assert(Await.result(resp).readableBytes === 8)
    val respFlags = Flags(Await.result(resp).readLong)
    assert(respFlags === flags)
  }
}
