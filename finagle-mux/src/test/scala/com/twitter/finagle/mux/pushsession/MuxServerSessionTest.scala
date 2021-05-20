package com.twitter.finagle.mux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message._
import com.twitter.finagle._
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Future, MockTimer, Promise, Time}
import org.scalatest.funsuite.AnyFunSuite

class MuxServerSessionTest extends AnyFunSuite {

  private val data = Buf.ByteArray.Owned((0 until 1024).map(_.toByte).toArray)

  private abstract class Ctx {
    lazy val mockTimer: MockTimer = new MockTimer

    lazy val params: Stack.Params = Mux.server.params + (param.Timer(mockTimer))

    lazy val decoder: MuxMessageDecoder =
      new FragmentDecoder(params[param.Stats].statsReceiver)

    lazy val messageWriter: MockMessageWriter = new MockMessageWriter

    lazy val handle: MockChannelHandle[ByteReader, Buf] = new MockChannelHandle[ByteReader, Buf]()

    lazy val service: Service[Request, Response] = Service.mk { req: Request =>
      Future.value(Response(req.body.concat(req.body)))
    }

    var serviceClosed = false

    private def proxyService = new ServiceProxy(service) {
      override def close(deadline: Time): Future[Unit] = {
        serviceClosed = true
        super.close(deadline)
      }
    }

    lazy val session: MuxServerSession = new MuxServerSession(
      params = params,
      h_decoder = decoder,
      h_messageWriter = messageWriter,
      handle = handle,
      service = proxyService
    )

    def sessionReceive(msg: Message): Unit = {
      val br = ByteReader(Message.encode(msg))
      session.receive(br)
    }
  }

  test("Dispatch requests") {
    new Ctx {
      sessionReceive(Tdispatch(2, Nil, Path.empty, Dtab.empty, data))
      // Service should have made a trip through the executor
      assert(1 == handle.serialExecutor.pendingTasks)
      handle.serialExecutor.executeAll()

      messageWriter.messages.dequeue() match {
        case RdispatchOk(2, Nil, body) => assert(data.concat(data) == body)
        case other => fail(s"Unexpected message: $other")
      }

      sessionReceive(Treq(2, None, data))
      // Service should have made a trip through the executor
      assert(1 == handle.serialExecutor.pendingTasks)
      handle.serialExecutor.executeAll()

      messageWriter.messages.dequeue() match {
        case RreqOk(2, body) => assert(data.concat(data) == body)
        case other => fail(s"Unexpected message: $other")
      }
    }
  }

  test("responds to pings") {
    new Ctx {
      sessionReceive(Tping(1))
      handle.serialExecutor.executeAll()
      assert(messageWriter.messages.dequeue() == Message.PreEncoded.Rping)

      sessionReceive(Tping(2))
      handle.serialExecutor.executeAll()
      val Rping(2) = messageWriter.messages.dequeue()
    }
  }

  test("Drains allow dispatches to finish") {
    new Ctx {
      val rep = Promise[Response]
      override lazy val service = Service.constant(rep)

      Time.withCurrentTimeFrozen { control =>
        session.close(5.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)
        val Tdrain(1) = messageWriter.messages.dequeue()

        // Send one message (we haven't sent the Rdrain)
        sessionReceive(Tdispatch(2, Nil, Path.empty, Dtab.empty, data))
        handle.serialExecutor.executeAll()

        // Don't quite hit the deadline
        control.advance(5.seconds - 1.millisecond)
        mockTimer.tick()
        handle.serialExecutor.executeAll()

        assert(session.status == Status.Busy)

        rep.setValue(Response(Nil, data))
        handle.serialExecutor.executeAll()

        val RdispatchOk(2, Seq(), `data`) = messageWriter.messages.dequeue()

        assert(session.status == Status.Busy)
        sessionReceive(Rdrain(1))
        handle.serialExecutor.executeAll()

        assert(session.status == Status.Closed)
        assert(handle.closedCalled)
        assert(serviceClosed)
      }
    }
  }

  test("Drains will cancel dispatches after timeout without receiving Rdrain") {
    new Ctx {
      val rep = Promise[Response]
      override lazy val service = Service.constant(rep)

      Time.withCurrentTimeFrozen { control =>
        session.close(5.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)
        val Tdrain(1) = messageWriter.messages.dequeue()

        // Send one message (we haven't sent the Rdrain)
        sessionReceive(Tdispatch(2, Nil, Path.empty, Dtab.empty, data))
        handle.serialExecutor.executeAll()

        // Hit the deadline
        control.advance(5.seconds)
        mockTimer.tick()
        handle.serialExecutor.executeAll()

        assert(session.status == Status.Closed)
        assert(rep.isInterrupted.isDefined)
        assert(handle.closedCalled)
        assert(serviceClosed)
      }
    }
  }

  test("Drains will cancel dispatches after timeout when receiving Rdrain") {
    new Ctx {
      val rep = Promise[Response]
      override lazy val service = Service.constant(rep)

      Time.withCurrentTimeFrozen { control =>
        session.close(5.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)
        val Tdrain(1) = messageWriter.messages.dequeue()

        // Send one message (we haven't sent the Rdrain)
        sessionReceive(Tdispatch(2, Nil, Path.empty, Dtab.empty, data))
        // And drain
        sessionReceive(Rdrain(1))
        handle.serialExecutor.executeAll()

        // Hit the deadline
        control.advance(5.seconds)
        mockTimer.tick()
        handle.serialExecutor.executeAll()

        assert(session.status == Status.Closed)
        assert(rep.isInterrupted.isDefined)
        assert(handle.closedCalled)
        assert(serviceClosed)
      }
    }
  }

  test("Nacks new requests after receiving Rdrain") {
    new Ctx {
      val rep = Promise[Response]
      override lazy val service = Service.constant(rep)

      Time.withCurrentTimeFrozen { control =>
        // need to put a dispatch in the queue
        sessionReceive(Tdispatch(2, Nil, Path.empty, Dtab.empty, data))

        session.close(5.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)
        val Tdrain(1) = messageWriter.messages.dequeue()

        // Send Rdrain
        sessionReceive(Rdrain(1))
        // Try a dispatch that should be nacked
        sessionReceive(Tdispatch(3, Nil, Path.empty, Dtab.empty, data))

        handle.serialExecutor.executeAll()

        val RdispatchNack(3, Nil) = messageWriter.messages.dequeue()

        // Hit the deadline
        control.advance(5.seconds)
        mockTimer.tick()
        handle.serialExecutor.executeAll()

        assert(session.status == Status.Closed)
        assert(rep.isInterrupted.isDefined)
      }
    }
  }

  test("Frees the service on handle close") {
    new Ctx {
      assert(session.status == Status.Open) // touch the lazy session to initiate it

      handle.onClosePromise.setDone()
      handle.serialExecutor.executeAll()

      // Service and handle should have been closed
      assert(handle.closedCalled)
      assert(serviceClosed)
      assert(session.status == Status.Closed)
    }
  }
}
