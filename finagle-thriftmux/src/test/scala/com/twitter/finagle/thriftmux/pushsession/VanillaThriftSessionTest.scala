package com.twitter.finagle.thriftmux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.{Service, Stack, Status, Thrift, ThriftMux, mux, param => fparam}
import com.twitter.finagle.mux.{ClientDiscardedRequestException, Request, Response}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.{OutputBuffer, thrift}
import com.twitter.finagle.util.ByteArrays
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Future, MockTimer, Promise, Return, Time}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class VanillaThriftSessionTest extends AnyFunSuite {

  private[this] val data = Buf.Utf8("data")

  private class Ctx {
    lazy val handle: MockChannelHandle[ByteReader, Buf] = new MockChannelHandle[ByteReader, Buf]()

    lazy val ttwitterHeader: Option[Buf] = None

    lazy val statsReceiver = new InMemoryStatsReceiver

    lazy val timer = new MockTimer

    lazy val params: Stack.Params = ThriftMux.BaseServerParams +
      fparam.Stats(statsReceiver) + fparam.Timer(timer)

    lazy val service: Service[mux.Request, mux.Response] = Service.mk { req =>
      Future.value(mux.Response(req.body.concat(req.body)))
    }

    lazy val session = new VanillaThriftSession(handle, ttwitterHeader, params, service)
  }

  test("can do a simple dispatch") {
    new Ctx {
      session.receive(ByteReader(data))
      handle.serialExecutor.executeAll()
      assert(session.status == Status.Open)

      assert(statsReceiver.gauges(Seq("pending")).apply() == 0.0f)
      val handle.SendOne(written, _) = handle.pendingWrites.dequeue()
      assert(written == data.concat(data))
    }
  }

  test("can pipeline dispatches") {
    new Ctx {
      val svcQueue = new mutable.Queue[Promise[Response]]()
      override lazy val service = Service.mk[Request, Response] { _ =>
        val p = Promise[Response]()
        svcQueue += p
        p
      }

      session.receive(ByteReader(data))
      session.receive(ByteReader(data))
      handle.serialExecutor.executeAll()
      assert(session.status == Status.Open)

      assert(statsReceiver.gauges(Seq("pending")).apply() == 2.0f)
      assert(handle.pendingWrites.isEmpty)

      val List(one, two) = svcQueue.toList

      // Satisfy these out of order
      two.setValue(Response(Buf.Utf8("second")))

      handle.serialExecutor.executeAll()
      assert(statsReceiver.gauges(Seq("pending")).apply() == 2.0f)
      assert(handle.pendingWrites.isEmpty)

      one.setValue(Response(Buf.Utf8("first")))

      handle.serialExecutor.executeAll()

      assert(statsReceiver.gauges(Seq("pending")).apply() == 0.0f)

      // Results are ordered correctly
      val handle.SendOne(first, _) = handle.pendingWrites.dequeue()
      assert(first == Buf.Utf8("first"))

      val handle.SendOne(second, _) = handle.pendingWrites.dequeue()
      assert(second == Buf.Utf8("second"))
    }
  }

  test("drains pending requests on close") {
    new Ctx {
      val svcQueue = new mutable.Queue[Promise[Response]]()
      override lazy val service = Service.mk[Request, Response] { _ =>
        val p = Promise[Response]()
        svcQueue += p
        p
      }

      Time.withCurrentTimeFrozen { _ =>
        session.receive(ByteReader(data))
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Open)

        session.close(Time.now + 30.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)

        assert(statsReceiver.gauges(Seq("pending")).apply() == 1.0f)
        assert(handle.pendingWrites.isEmpty)

        svcQueue.dequeue().setValue(Response(data))
        handle.serialExecutor.executeAll()

        assert(statsReceiver.gauges(Seq("pending")).apply() == 0.0f)
        val handle.SendOne(first, thunk) = handle.pendingWrites.dequeue()
        assert(first == data)
        thunk(Return.Unit)

        assert(session.status == Status.Closed)
        assert(handle.closedCalled)
      }
    }
  }

  test("drains pending writes on close") {
    new Ctx {
      Time.withCurrentTimeFrozen { _ =>
        session.receive(ByteReader(data))
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Open)

        // should have a write flushing
        session.close(Time.now + 30.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)

        assert(statsReceiver.gauges(Seq("pending")).apply() == 0.0f)
        assert(handle.pendingWrites.size == 1)

        val handle.SendOne(msg, thunk) = handle.pendingWrites.dequeue()
        assert(msg == data.concat(data))
        thunk(Return.Unit)

        assert(session.status == Status.Closed)
        assert(handle.closedCalled)
      }
    }
  }

  test("forces closed after the deadline has passed") {
    Time.withCurrentTimeFrozen { timeControl =>
      new Ctx {
        val p = Promise[Response]()
        override lazy val service = Service.mk[Request, Response] { _ => p }

        session.receive(ByteReader(data))
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Open)

        session.close(Time.now + 30.seconds)
        handle.serialExecutor.executeAll()
        assert(session.status == Status.Busy)

        assert(statsReceiver.gauges(Seq("pending")).apply() == 1.0f)
        assert(handle.pendingWrites.isEmpty)

        timeControl.advance(31.seconds)
        timer.tick()
        assert(handle.closedCalled)
        handle.onClosePromise.setDone()
        handle.serialExecutor.executeAll()

        assert(
          statsReceiver.gauges.get(Seq("pending")).isEmpty
        ) // should have been removed in the close
        assert(handle.pendingWrites.isEmpty)
        p.isInterrupted match {
          case Some(_: ClientDiscardedRequestException) => // nop OK
          case other =>
            fail(s"Expected the service to have interrupted dispatches. Instead found $other")
        }
        assert(session.status == Status.Closed)
      }
    }
  }

  test("expects and adds a TTwitter header if configured to use the TTwitter protocol") {
    new Ctx {
      override lazy val ttwitterHeader: Option[Buf] = Some(Buf.Utf8("header"))

      val header = new thrift.RequestHeader
      header.setClient_id(new thrift.ClientId("clientId"))

      val protocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory
      val msg = Buf.ByteArray.Owned(
        ByteArrays.concat(
          OutputBuffer.messageToArray(header, protocolFactory),
          "body".getBytes(StandardCharsets.UTF_8)
        )
      )

      session.receive(ByteReader(msg))
      handle.serialExecutor.executeAll()

      val handle.SendOne(first, _) = handle.pendingWrites.dequeue()
      assert(first == Buf.Utf8("headerbodybody"))
    }
  }
}
