package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Annotation, BufferingTracer, Flags, Record, SpanId, Trace, TraceId}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ClientServerSpec extends SpecificationWithJUnit with Mockito {
  "Client+Server" should {
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

    "end-to-end with tracing: client-to-service" in {
      val p = new Promise[ChannelBuffer]
      service(buf(1)) returns p

      there was no(service)(any)
      val id = TraceId(Some(SpanId(1)), Some(SpanId(2)), SpanId(3), None)
      val f = Trace.unwind {
        Trace.setId(id)
        client(buf(1))
      }
      there was one(service)(buf(1))
      f.poll must beNone
      p.setValue(buf(2))
      f.poll must beSome(Return(buf(2)))

      val ia = new InetSocketAddress(0)
      val recs = tracer.toSeq.sortBy(_.timestamp)
      recs must beLike {
        case Seq(
          Record(`id`, _, Annotation.ClientSend(), None),
          Record(`id`, _, Annotation.ServerRecv(), None),
          Record(`id`, _, Annotation.ServerAddr(ia), None),
          Record(`id`, _, Annotation.ServerSend(), None),
          Record(`id`, _, Annotation.ClientRecv(), None)) => true
      }
    }

    "handle concurrent requests, handling out of order replies" in {
      val p1, p2, p3 = new Promise[ChannelBuffer]
      service(buf(1)) returns p1
      service(buf(2)) returns p2
      service(buf(3)) returns p3

      val f1 = client(buf(1))
      val f2 = client(buf(2))
      val f3 = client(buf(3))

      for (i <- 1 to 3)
        there was one(service)(buf(i.toByte))

      for (f <- Seq(f1, f2, f3))
        f.poll must beNone

      p2.setValue(buf(20))
      f1.poll must beNone
      f2.poll must beSome(Return(buf(20)))
      f3.poll must beNone

      p1.setValue(buf(10))
      f1.poll must beSome(Return(buf(10)))
      f3.poll must beNone

      p3.setValue(buf(9))
      f3.poll must beSome(Return(buf(9)))
    }

    "server respond to pings" in {
      client.ping().isDefined must beTrue
    }

    "server nacks new requests after draining" in {
      val p1 = new Promise[ChannelBuffer]
      service(buf(1)) returns p1

      val f1 = client(buf(1))
      there was one(service)(buf(1))
      server.close(Time.now)
      f1.poll must beNone
      client(buf(2)).poll must beSome(Throw(RequestNackedException))
      there was no(service)(buf(2))

      p1.setValue(buf(123))
      f1.poll must beSome(Return(buf(123)))
    }

    "handle errors" in {
      service(buf(1)) returns Future.exception(new Exception("sad panda"))
      client(buf(1)).poll must beSome(
        Throw(ServerApplicationError("java.lang.Exception: sad panda")))
    }

    "propagate interrupts" in {
      val p = new Promise[ChannelBuffer]
      service(buf(1)) returns p
      val f = client(buf(1))

      f.poll must beNone
      p.isInterrupted must beNone

      val exc = new Exception("sad panda")
      f.raise(exc)
      p.isInterrupted must beSome(
        ClientDiscardedRequestException("java.lang.Exception: sad panda"))

      f.poll must beSome(Throw(exc))
    }

    "propagate trace ids" in {
      service(any) answers { args =>
        val traceId = ChannelBuffers.wrappedBuffer(
          Trace.id.toString.getBytes(CharsetUtil.UTF_8))
        Future.value(traceId)
      }

      val id = Trace.nextId
      val resp = Trace.unwind {
        Trace.setId(id)
        client(buf(1))
      }
      resp.poll must beSomething
      val respBuf = Await.result(resp)
      val respArr = new Array[Byte](respBuf.readableBytes)
      respBuf.readBytes(respArr)
      val respStr = new String(respArr, CharsetUtil.UTF_8)
      respStr must be_==(id.toString)
    }

    "propagate trace flags" in {
      service(any) answers { args =>
        val buf = ChannelBuffers.directBuffer(8)
        buf.writeLong(Trace.id.flags.toLong)
        Future.value(buf)
      }

      val flags = Flags().setDebug
      val id = Trace.nextId.copy(flags=flags)
      val resp = Trace.unwind {
        Trace.setId(id)
        val p = client(buf(1))
        p
      }
      resp.poll must beSomething
      Await.result(resp).readableBytes must be_==(8)
      val respFlags = Flags(Await.result(resp).readLong)
      respFlags must be_==(flags)
    }
  }
}
