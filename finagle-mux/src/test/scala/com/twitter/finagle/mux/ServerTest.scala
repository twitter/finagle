package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.mux.lease.exp.{Lessor, nackOnExpiredLease}
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.finagle.{Dtab, Failure, Path, Service}
import com.twitter.io.Buf.Utf8
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration, Future, Promise, Return, Throw, Time}
import java.security.cert.X509Certificate
import java.net.SocketAddress
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

class ServerTest extends FunSuite with MockitoSugar with AssertionsForJUnit {

  private class LeaseCtx {
    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]
    val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)
    val service = mock[Service[Request, Response]]
    val lessor = mock[Lessor]
    val server =
      ServerDispatcher.newRequestResponse(transport, service, lessor, NullTracer, NullStatsReceiver)

    def issue(lease: Duration) {
      val m = serverToClient.poll()
      assert(!m.isDefined)
      server.issue(lease)
      assert(m.isDefined)
      checkFuture(m, Message.Tlease(lease))
    }

    def demonstrateNack() {
      val m = serverToClient.poll()
      assert(!m.isDefined)
      clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))
      assert(m.isDefined)
      checkFuture(m, Message.RdispatchNack(0, Seq.empty))
    }

    def demonstrateNoNack() {
      val m = serverToClient.poll()
      assert(!m.isDefined)
      clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))
      assert(!m.isDefined)
    }
  }

  private[this] def checkFuture(actual: Future[Message], expected: Message) {
    actual.poll match {
      case Some(Return(msg)) => assert(msg == expected)
      case _ => fail()
    }
  }

  test("register/unregister with lessor") {
    val ctx = new LeaseCtx
    import ctx._

    verify(lessor).register(server)
    verify(lessor, never()).unregister(server)
    clientToServer.fail(new Exception)
    verify(lessor).unregister(server)
  }

  test("propagate leases") {
    val ctx = new LeaseCtx
    import ctx._

    val m = serverToClient.poll()
    assert(!m.isDefined)
    server.issue(123.milliseconds)
    assert(m.isDefined)
    assert(Await.result(m, 5.seconds) == Message.Tlease(123.milliseconds))
  }

  test("nack on 0 leases") {
    val ctx = new LeaseCtx
    import ctx._

    nackOnExpiredLease.parse("true")
    issue(Duration.Zero)

    demonstrateNack()
  }

  test("don't nack on > 0 leases") {
    val ctx = new LeaseCtx
    import ctx._

    nackOnExpiredLease.parse("true")
    issue(1.minute)
    demonstrateNoNack()
  }

  test("unnack again after a > 0 lease") {
    val ctx = new LeaseCtx
    import ctx._

    nackOnExpiredLease.parse("true")

    issue(Duration.Zero)
    demonstrateNack()

    issue(1.minute)
    demonstrateNoNack()
  }

  test("does not leak pending on failures") {
    val p = new Promise[Response]
    val svc = Service.mk[Request, Response](_ => p)

    val msg = Message.Treq(tag = 9, traceId = None, Buf.Empty)

    val trans = mock[Transport[Message, Message]]

    when(trans.onClose)
      .thenReturn(new Promise[Throwable])

    when(trans.read())
      .thenReturn(Future.value(msg))
      .thenReturn(Future.never)

    when(trans.write(any[Message]))
      .thenReturn(Future.Done)

    when(trans.peerCertificate)
      .thenReturn(None)

    val dispatcher =
      ServerDispatcher.newRequestResponse(trans, svc, Lessor.nil, NullTracer, NullStatsReceiver)
    assert(dispatcher.npending() == 1)

    p.updateIfEmpty(Throw(new RuntimeException("welp")))

    assert(dispatcher.npending() == 0)
  }

  test("nack on restartable failures") {
    val svc = new Service[Request, Response] {
      def apply(req: Request) = Future.exception(Failure.rejected("overloaded!"))
    }

    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]
    val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)
    val server =
      ServerDispatcher.newRequestResponse(transport, svc, Lessor.nil, NullTracer, NullStatsReceiver)

    clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))

    val reply = serverToClient.poll()
    assert(reply.isDefined)
    assert(Await.result(reply, 5.seconds).isInstanceOf[Message.RdispatchNack])
  }

  test("Transmit Failure flags via MuxFailures in response context") {
    val svc = new Service[Request, Response] {
      def apply(req: Request) = Future.exception(Failure("Super fail", Failure.NonRetryable))
    }

    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]
    val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)
    val server =
      ServerDispatcher.newRequestResponse(transport, svc, Lessor.nil, NullTracer, NullStatsReceiver)

    clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))

    val reply = serverToClient.poll()
    assert(reply.isDefined)

    Await.result(reply, 5.seconds) match {
      case Message.RdispatchError(_, ctxts, _) =>
        assert(ctxts.equals(MuxFailure(MuxFailure.NonRetryable).contexts))
      case _ => fail("Reply was not an RdispatchError")
    }
  }

  test("drains properly before closing the socket") {
    Time.withCurrentTimeFrozen { ctl =>
      val buf = Buf.Utf8("OK")
      val serverToClient = new AsyncQueue[Message]
      val clientToServer = new AsyncQueue[Message]
      val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)

      val p = Promise[Response]
      var req: Request = null
      val server = ServerDispatcher.newRequestResponse(
        transport,
        Service.mk { _req: Request =>
          req = _req
          p
        }
      )

      clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, buf))
      // one outstanding request

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      clientToServer.offer(Message.Rdrain(1)) // client draining

      assert(!drain.isDefined) // one outstanding request

      p.setValue(Response(Nil, Buf.Utf8("KO")))

      assert(drain.isDefined) // zero outstanding requests
    }
  }

  test("drains properly before closing the socket with two outstanding") {
    Time.withCurrentTimeFrozen { ctl =>
      val serverToClient = new AsyncQueue[Message]
      val clientToServer = new AsyncQueue[Message]
      val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)

      var promises: List[Promise[Response]] = Nil
      val server = ServerDispatcher.newRequestResponse(transport, Service.mk { _: Request =>
        val p = Promise[Response]()
        promises ::= p
        p
      })

      clientToServer.offer(Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))
      // one outstanding request

      clientToServer.offer(Message.Tdispatch(1, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))
      // two outstanding requests

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      clientToServer.offer(Message.Rdrain(1)) // client draining

      assert(!drain.isDefined) // two outstanding request
      assert(server.npending() == 2) // two outstanding request

      promises(0).setValue(Response.empty)

      assert(server.npending() == 1) // one outstanding request
      assert(!drain.isDefined) // one outstanding request

      promises(1).setValue(Response.empty)

      assert(server.npending() == 0) // zero outstanding request
      assert(drain.isDefined) // zero outstanding requests
    }
  }

  test("closes properly without outstanding requests") {
    Time.withCurrentTimeFrozen { ctl =>
      val serverToClient = new AsyncQueue[Message]
      val clientToServer = new AsyncQueue[Message]
      val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)

      val server = ServerDispatcher.newRequestResponse(transport, Service.mk(req => Future.???))

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      val Some(Return(tdrain)) = serverToClient.poll.poll
      val Message.Tdrain(tag) = tdrain

      assert(!drain.isDefined) // client hasn't acked
      clientToServer.offer(Message.Rdrain(tag)) // client draining
      assert(drain.isDefined) // safe to shut down
    }
  }

  private[this] class Server(
    svc: Service[Request, Response],
    peerCert: Option[X509Certificate] = None,
    remoteAddr: SocketAddress = null
  ) {
    val serverToClient = new AsyncQueue[Message]
    val clientToServer = new AsyncQueue[Message]
    val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer) {
      override def peerCertificate = peerCert
      override val remoteAddress = remoteAddr
    }
    def ping() = Future.Done

    val server = ServerDispatcher.newRequestResponse(transport, svc)

    def request(msg: Message): Unit = clientToServer.offer(msg)
    def read(): Future[Message] = serverToClient.poll
  }

  test("starts nacking only after receiving an rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._

      val server = new Server(Service.mk { req: Request =>
        Future.value(Response.empty)
      })

      server.request( // request before closing
        Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty)
      )
      assert(server.read().isDefined)

      val drain = server.server.close(Time.Top) // synchronously sends drain request to client

      val Some(Return(tdrain)) = server.read().poll
      val Tdrain(tag) = tdrain

      server.request( // request after sending tdrain, before getting rdrain
        Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty)
      )
      assert(server.read().isDefined)

      assert(!drain.isDefined) // client hasn't acked
      server.request(Rdrain(tag)) // client draining

      assert(drain.isDefined) // safe to shut down

      server.request( // request after closing down
        Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, Buf.Empty)
      )
      val Some(Return(rdrain)) = server.read().poll
      assert(rdrain.isInstanceOf[RdispatchNack])
    }
  }

  test("propagates peer certificates") {
    val mockCert = mock[X509Certificate]
    val okResponse = Response(Nil, Utf8("ok"))
    val failResponse = Response(Nil, Utf8("fail"))

    val testService = new Service[Request, Response] {
      override def apply(request: Request): Future[Response] = Future.value {
        if (Contexts.local.get(Transport.peerCertCtx) == Some(mockCert)) okResponse
        else failResponse
      }
    }

    val tag = 3
    val server = new Server(testService, Some(mockCert))
    val req = Message.Treq(tag, None, Request.empty.body)

    server.request(req)
    val Some(Return(res)) = server.read().poll

    assert(res == Message.RreqOk(tag, okResponse.body))
  }

  test("propagates remote address to service dispatch") {
    val mockAddr = mock[SocketAddress]
    val okResponse = Response(Nil, Utf8("ok"))
    val failResponse = Response(Nil, Utf8("fail"))

    val testService = new Service[Request, Response] {
      override def apply(request: Request): Future[Response] = {
        val remoteInfo = Contexts.local.get(RemoteInfo.Upstream.AddressCtx)
        val res = if (remoteInfo == Some(mockAddr)) okResponse else failResponse
        Future.value(res)
      }
    }

    val tag = 3
    val server = new Server(testService, None, mockAddr)
    val req = Message.Treq(tag, None, Request.empty.body)

    server.request(req)
    val Some(Return(res)) = server.read().poll

    assert(res == Message.RreqOk(tag, okResponse.body))
  }

  test("interrupts writes on Tdiscarded") {
    val writep = new Promise[Unit]
    writep.setInterruptHandler { case exc => writep.setException(exc) }

    val clientToServer = new AsyncQueue[Message]
    val transport = new QueueTransport(new AsyncQueue[Message], clientToServer) {
      override def write(in: Message) = writep
    }

    val svc = Service.mk { req: Request =>
      Future.value(Response.empty)
    }
    val server = ServerDispatcher.newRequestResponse(transport, svc)

    clientToServer.offer(Message.Tdispatch(20, Seq.empty, Path.empty, Dtab.empty, Buf.Empty))

    clientToServer.offer(Message.Tdiscarded(20, "timeout"))

    intercept[ClientDiscardedRequestException] { Await.result(writep, 1.second) }
  }

  test("duplicate tags are serviced") {
    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]
    val writep = new Promise[Unit]

    val transport = new QueueTransport(serverToClient, clientToServer) {
      override def write(in: Message) = writep.before {
        super.write(in)
      }
    }

    val sr = new InMemoryStatsReceiver

    val svc = Service.mk { req: Request =>
      Future.value(Response.empty)
    }
    val server = ServerDispatcher.newRequestResponse(transport, svc, Lessor.nil, NullTracer, sr)

    val msg = Message.Tdispatch(tag = 10, Seq.empty, Path.empty, Dtab.empty, Buf.Empty)

    clientToServer.offer(msg)
    clientToServer.offer(msg)

    assert(sr.counters(Seq("duplicate_tag")) == 1)

    writep.setDone()

    assert(Await.result(serverToClient.poll().liftToTry, 30.seconds).isReturn)
    assert(Await.result(serverToClient.poll().liftToTry, 30.seconds).isReturn)
  }
}
