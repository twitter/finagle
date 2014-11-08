package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.mux.Message.Treq
import com.twitter.finagle.mux.lease.exp.{Lessor, nackOnExpiredLease}
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.finagle.{Dtab, Service}
import com.twitter.io.Charsets
import com.twitter.util.{Return, Future, Time, Duration, Promise}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.junit.runner.RunWith
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ServerTest extends FunSuite with MockitoSugar {

  private class Ctx {
    val clientToServer = new AsyncQueue[ChannelBuffer]
    val serverToClient = new AsyncQueue[ChannelBuffer]
    val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)
    val service = mock[Service[ChannelBuffer, ChannelBuffer]]
    val lessor = mock[Lessor]
    val server = new ServerDispatcher(transport, service, true, lessor)

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
      clientToServer.offer(Message.encode(
        Message.Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      assert(m.isDefined)
      checkFuture(m, Message.RdispatchNack(0, Seq.empty))
    }

    def demonstrateNoNack() {
      val m = serverToClient.poll()
      assert(!m.isDefined)
      clientToServer.offer(Message.encode(
        Message.Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      assert(!m.isDefined)
    }
  }

  def checkFuture(actual: Future[ChannelBuffer], expected: Message) {
    actual.poll match {
      case Some(Return(bytes)) => assert(Message.decode(bytes) === expected)
      case _ => fail()
    }
  }

  test("register/unregister with lessor") {
    val ctx = new Ctx
    import ctx._

    verify(lessor).register(server)
    verify(lessor, never()).unregister(server)
    clientToServer.fail(new Exception)
    verify(lessor).unregister(server)
  }

  test("propagate leases") {
    val ctx = new Ctx
    import ctx._

    val m = serverToClient.poll()
    assert(!m.isDefined)
    server.issue(123.milliseconds)
    assert(m.isDefined)
    assert(Message.decode(m()) === Message.Tlease(123.milliseconds))
  }

  test("nack on 0 leases") {
    val ctx = new Ctx
    import ctx._

    nackOnExpiredLease.parse("true")
    issue(Duration.zero)

    demonstrateNack()
  }

  test("don't nack on > 0 leases") {
    val ctx = new Ctx
    import ctx._

    nackOnExpiredLease.parse("true")

    issue(1.millisecond)

    demonstrateNoNack()
  }


  test("unnack again after a > 0 lease") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = new Ctx
      import ctx._

      nackOnExpiredLease.parse("true")

      issue(Duration.zero)


      demonstrateNack()

      ctl.advance(2.seconds)
      issue(1.second)

      demonstrateNoNack()
    }
  }

  test("does not leak pending on failures") {
    val ctx = new Ctx
    import ctx._

    val p = new Promise[ChannelBuffer]
    val svc = new Service[ChannelBuffer, ChannelBuffer] {
      def apply(request: ChannelBuffer): Future[ChannelBuffer] = p
    }

    val encodedMsg = Message.encode(
      Treq(tag = 9, traceId = None, ChannelBuffers.EMPTY_BUFFER))

    val trans = mock[Transport[ChannelBuffer, ChannelBuffer]]
    when(trans.onClose).thenReturn(new Promise[Throwable])
    when(trans.read())
      .thenReturn(Future.value(encodedMsg))
      .thenReturn(Future.never)

    val dispatcher = new ServerDispatcher(trans, svc, true, lessor)
    assert(dispatcher.npending() === 1)

    // fulfill the promise with a failure
    p.setException(new RuntimeException("welp"))

    assert(dispatcher.npending() === 0)
  }

  test("drains properly before closing the socket") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)

      val p = Promise[ChannelBuffer]
      var req: ChannelBuffer = null
      val server = new ServerDispatcher(transport, Service.mk { buf: ChannelBuffer =>
        req = buf
        p
      }, true)

      clientToServer.offer(encode(Tdispatch(0, Seq.empty, "", Dtab.empty, buf)))
      // one outstanding request

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      clientToServer.offer(encode(Rdrain(1))) // client draining

      assert(!drain.isDefined) // one outstanding request

      p.setValue(ChannelBuffers.copiedBuffer(req.toString("UTF-8").reverse, Charsets.Utf8))

      assert(drain.isDefined) // zero outstanding requests
    }
  }

  test("drains properly before closing the socket with two outstanding") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)

      var promises: List[Promise[ChannelBuffer]] = Nil
      val server = new ServerDispatcher(transport, Service.mk { buf: ChannelBuffer =>
        val p = Promise[ChannelBuffer]()
        promises ::= p
        p
      }, true)

      clientToServer.offer(encode(
        Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      // one outstanding request

      clientToServer.offer(encode(
        Tdispatch(1, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      // two outstanding requests

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      clientToServer.offer(encode(Rdrain(1))) // client draining

      assert(!drain.isDefined) // two outstanding request
      assert(server.npending() === 2) // two outstanding request

      promises(0).setValue(ChannelBuffers.EMPTY_BUFFER)

      assert(server.npending() === 1) // one outstanding request
      assert(!drain.isDefined) // one outstanding request

      promises(1).setValue(ChannelBuffers.EMPTY_BUFFER)

      assert(server.npending() === 0) // zero outstanding request
      assert(drain.isDefined) // zero outstanding requests
    }
  }

  test("closes properly without outstanding requests") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)

      val server = new ServerDispatcher(transport, Service.mk { buf: ChannelBuffer =>
        Future { ??? }
      }, true)

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      val Some(Return(tdrain)) = serverToClient.poll.poll
      val Tdrain(tag) = Message.decode(tdrain)

      assert(!drain.isDefined) // client hasn't acked
      clientToServer.offer(encode(Rdrain(tag))) // client draining
      assert(drain.isDefined) // safe to shut down
    }
  }

  test("starts nacking only after receiving an rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)

      val server = new ServerDispatcher(transport, Service.mk { buf: ChannelBuffer =>
        Future.value(ChannelBuffers.EMPTY_BUFFER)
      }, true)

      clientToServer.offer(Message.encode( // request before closing
        Message.Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      assert(serverToClient.poll.isDefined)

      val drain = server.close(Time.Top) // synchronously sends drain request to client

      val Some(Return(tdrain)) = serverToClient.poll.poll
      val Tdrain(tag) = Message.decode(tdrain)

      clientToServer.offer(Message.encode( // request after sending tdrain, before getting rdrain
        Message.Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      assert(serverToClient.poll.isDefined)

      assert(!drain.isDefined) // client hasn't acked
      clientToServer.offer(encode(Rdrain(tag))) // client draining

      assert(drain.isDefined) // safe to shut down

      clientToServer.offer(Message.encode( // request after closing down
        Message.Tdispatch(0, Seq.empty, "", Dtab.empty, ChannelBuffers.EMPTY_BUFFER)))
      val Some(Return(rdrain)) = serverToClient.poll.poll
      assert(decode(rdrain).isInstanceOf[RdispatchNack])
    }
  }
}
