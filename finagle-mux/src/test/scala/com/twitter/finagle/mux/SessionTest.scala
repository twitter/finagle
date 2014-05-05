package com.twitter.finagle.mux.exp

import com.twitter.finagle.mux.{RequestNackedException, ServerApplicationError, ClientDiscardedRequestException, MuxContext}
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.tracing.{
  Annotation, BufferingTracer, Flags, Record, SpanId, Trace, TraceId}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Service, ContextHandler}
import com.twitter.io.{Charsets, Buf}
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite}

class MuxServiceImpl() extends MuxService with MockitoSugar {
  val service = mock[Service[Buf, Buf]]
  val receivedPing = new Promise[Unit]
  val receivedDrain = new Promise[Unit]
  val messages = new AsyncQueue[Buf]

  def apply(buf: Buf) = service(buf)
  def send(buf: Buf) = {
    messages.offer(buf)
    Future.Unit
  }
  def ping() = {
    receivedPing.setDone
    Future.Unit
  }
  def drain() = {
    receivedDrain.setDone
    Future.Unit
  }
}

private[mux] class SessionTest(rolesReversed: Boolean) extends FunSuite with OneInstancePerTest with MockitoSugar {
  def buf(b: Byte*) = Buf.ByteArray(b:_*)

  val tracer = new BufferingTracer
  Trace.pushTracer(tracer)
  val connectorToListener = new AsyncQueue[ChannelBuffer]
  val listenerToConnector = new AsyncQueue[ChannelBuffer]

  val connectorTransport =
    new QueueTransport(writeq=connectorToListener, readq=listenerToConnector)
  val listenerTransport =
    new QueueTransport(writeq=listenerToConnector, readq=connectorToListener)

  val listenerImpl = new MuxServiceImpl()
  val connectorImpl = new MuxServiceImpl()

  val connectorClientDispatcher = new ClientDispatcher(connectorTransport)
  val listenerClientDispatcher = new ClientDispatcher(listenerTransport)

  val connectorSession = new Session(connectorClientDispatcher, connectorImpl, connectorTransport)
  val listenerSession = new Session(listenerClientDispatcher, listenerImpl, listenerTransport)

  val (client, clientImpl, server, serverImpl) =
    if (rolesReversed) {
      (listenerClientDispatcher, listenerImpl, connectorClientDispatcher, connectorImpl)
    } else {
      (connectorClientDispatcher, connectorImpl, listenerClientDispatcher, listenerImpl)
    }

  val service = serverImpl.service

  test("handle concurrent requests, handling out of order replies") {
    val p1, p2, p3 = new Promise[Buf]
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
    assert(serverImpl.receivedPing.isDefined)
  }

  test("server nacks new requests after draining") {
    val p1 = new Promise[Buf]
    when(service(buf(1))).thenReturn(p1)

    val f1 = client(buf(1))
    verify(service)(buf(1))
    server.drain()
    assert(clientImpl.receivedDrain.isDefined)
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
    val p = new Promise[Buf]
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

  test("end-to-end with tracing: client-to-service") {
    val p = new Promise[Buf]
    when(service(buf(1))).thenReturn(p)

    verify(service, never())(any[Buf])
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

  test("propagate trace ids") {
    when(service(any[Buf])).thenAnswer(
      new Answer[Future[Buf]]() {
        def answer(invocation: InvocationOnMock) = {
          val traceId = Buf.ByteArray(
            Trace.id.toString.getBytes(Charsets.Utf8))
          Future.value(traceId)
        }
      }
    )

    val id = Trace.nextId
    val resp = Trace.unwind {
      Trace.setId(id)
      client(buf(1))
    }
    assert(resp.poll.isDefined)
    val respBuf = Await.result(resp)
    val respArr = new Array[Byte](respBuf.length)
    respBuf.write(respArr, 0)
    val respStr = new String(respArr, Charsets.Utf8)
    assert(respStr === id.toString)
  }

  test("propagate trace flags") {
    when(service(any[Buf])).thenAnswer(
      new Answer[Future[Buf]] {
        def answer(invocation: InvocationOnMock) = {
          val buf = ChannelBuffers.directBuffer(8)
          buf.writeLong(Trace.id.flags.toLong)
          Future.value(ChannelBufferBuf(buf))
        }
      }
    )

    val flags = Flags().setDebug
    val id = Trace.nextId.copy(flags=flags)
    val resp = Trace.unwind {
      Trace.setId(id)
      val p = client(buf(1))
      p
    }
    assert(resp.poll.isDefined)
    val respBuf = Await.result(resp)
    assert(respBuf.length === 8)
    val respArr = new Array[Byte](respBuf.length)
    respBuf.write(respArr, 0)
    val byteBuffer = ByteBuffer.wrap(respArr)
    val respFlags = Flags(byteBuffer.getLong)
    assert(respFlags === flags)
  }

  test("Transmits request contexts") {
    when(service(any[Buf])).thenReturn(
      Future.value(Buf.Empty))

    MuxContext.handled = Seq.empty
    MuxContext.buf = Buf.ByteArray(1,2,3,4)

    var f = client(Buf.Empty)
    assert(f.isDefined)
    Await.result(f)
    assert(MuxContext.handled === Seq(Buf.ByteArray(1,2,3,4)))

    MuxContext.buf = Buf.ByteArray(9,8,7,6)
    f = client(Buf.Empty)
    assert(f.isDefined)
    Await.result(f)

    assert(MuxContext.handled === Seq(
      Buf.ByteArray(1,2,3,4), Buf.ByteArray(9,8,7,6)))
  }

  test("Passes messages") {
    val a = serverImpl.messages.poll
    val b = serverImpl.messages.poll
    val c = serverImpl.messages.poll
    assert(a.poll === None)
    assert(b.poll === None)
    assert(c.poll === None)

    client.send(buf(1))
    assert(a.poll === Some(Return(buf(1))))
    assert(b.poll === None)
    assert(c.poll === None)

    client.send(buf(2))
    assert(b.poll === Some(Return(buf(2))))
    assert(c.poll === None)

    client.send(buf(3))
    assert(c.poll === Some(Return(buf(3))))

    val x = clientImpl.messages.poll
    val y = clientImpl.messages.poll
    val z = clientImpl.messages.poll
    assert(x.poll === None)
    assert(y.poll === None)
    assert(z.poll === None)

    server.send(buf(4))
    assert(x.poll === Some(Return(buf(4))))
    assert(y.poll === None)
    assert(z.poll === None)

    server.send(buf(5))
    assert(y.poll === Some(Return(buf(5))))
    assert(z.poll === None)

    server.send(buf(6))
    assert(z.poll === Some(Return(buf(6))))

  }
}


@RunWith(classOf[JUnitRunner])
class ConnectorClientTest extends SessionTest(false)

@RunWith(classOf[JUnitRunner])
class ListenerClientTest extends SessionTest(true)
