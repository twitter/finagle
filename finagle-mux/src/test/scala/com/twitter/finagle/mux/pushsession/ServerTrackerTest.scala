package com.twitter.finagle.mux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.client.BackupRequestFilter
import com.twitter.finagle.pushsession.utils.DeferredExecutor
import com.twitter.finagle.mux.pushsession.MessageWriter.DiscardResult
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.lease.exp.nackOnExpiredLease
import com.twitter.finagle.mux.transport.Message._
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.Dtab
import com.twitter.finagle.Path
import com.twitter.finagle.Service
import com.twitter.finagle.mux
import com.twitter.io.Buf
import com.twitter.util._
import java.net.InetSocketAddress
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class ServerTrackerTest extends AnyFunSuite {

  def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  private val data = Buf.ByteArray.Owned((0 until 1024).map(_.toByte).toArray)

  private abstract class Ctx {
    def service: Service[Request, Response] = Service.mk[mux.Request, mux.Response] { req =>
      Future.value(mux.Response(req.body))
    }

    val remoteAddress = new InetSocketAddress("1.1.1.1", 8080)

    val executor = new DeferredExecutor
    val messageWriter = new MockMessageWriter
    val locals = () => Local.letClear(Local.save())
    val statsReceiver = new InMemoryStatsReceiver
    val lessor = Lessor.nil

    lazy val tracker = new ServerTracker(
      executor,
      locals,
      service,
      messageWriter,
      lessor,
      statsReceiver,
      remoteAddress
    )
  }

  test("Dispatches Treq's") {
    new Ctx {
      var dispatch: Option[Request] = None
      override val service: Service[Request, Response] = Service.mk { req: Request =>
        dispatch = Some(req)
        super.service(req)
      }

      tracker.dispatch(Treq(tag = 2, traceId = None, req = data))

      assert(dispatch.get.body == data)

      executor.executeAll()

      val RreqOk(2, `data`) = messageWriter.messages.dequeue()
    }
  }

  test("Dispatches Tdispatch's") {
    new Ctx {
      var dispatch: Option[Request] = None
      override val service: Service[Request, Response] = Service.mk { req: Request =>
        dispatch = Some(req)
        super.service(req)
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      assert(dispatch.get.body == data)

      executor.executeAll()

      val RdispatchOk(2, contexts, `data`) = messageWriter.messages.dequeue()
      assert(contexts.isEmpty)
    }
  }

  test("Clears pending responses") {
    new Ctx {
      val p = Promise[Response]()
      override val service: Service[Request, Response] = Service.const(p)

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      assert(tracker.lessee.npending == 1)
      assert(!p.isDefined)

      tracker.interruptOutstandingDispatches(new CancelledRequestException(new Exception))
      val Some(ex) = p.isInterrupted

      assert(ex.isInstanceOf[CancelledRequestException])
      assert(tracker.lessee.npending == 0)
    }
  }

  test("Discards dispatches that haven't returned") {
    new Ctx {
      val p = Promise[Response]()
      override val service: Service[Request, Response] = Service.const(p)

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      assert(tracker.lessee.npending == 1)
      assert(!p.isDefined)

      tracker.discarded(2, "Foo")

      val Some(ex) = p.isInterrupted

      assert(ex.isInstanceOf[ClientDiscardedRequestException])
      assert(tracker.lessee.npending == 0)

      val Rdiscarded(2) = messageWriter.messages.dequeue()

      // If the promise still completes, its result is just discarded
      p.setValue(Response(data))
      executor.executeAll()

      assert(tracker.lessee.npending == 0)
      assert(messageWriter.messages.isEmpty)
    }
  }

  test("Discards dispatches that haven't returned and should be ignorable") {
    new Ctx {
      val p = Promise[Response]()
      override val service: Service[Request, Response] = Service.const(p)

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      assert(tracker.lessee.npending == 1)
      assert(!p.isDefined)

      tracker.discarded(2, BackupRequestFilter.SupersededRequestFailureWhy)

      val ex = intercept[ClientDiscardedRequestException] {
        throw p.isInterrupted.get
      }

      assert(ex.flags == (FailureFlags.Interrupted | FailureFlags.Ignorable))
      assert(tracker.lessee.npending == 0)

      val Rdiscarded(2) = messageWriter.messages.dequeue()

      // If the promise still completes, its result is just discarded
      p.setValue(Response(data))
      executor.executeAll()

      assert(tracker.lessee.npending == 0)
      assert(messageWriter.messages.isEmpty)
    }
  }

  test("Discards dispatches that have returned but haven't been written") {
    new Ctx {
      override val messageWriter = new MockMessageWriter {
        override def removeForTag(id: Int): DiscardResult = DiscardResult.Unwritten
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      val RdispatchOk(2, _, _) = messageWriter.messages.dequeue()

      tracker.discarded(2, "Foo")
      assert(tracker.lessee.npending == 0)

      val Rdiscarded(2) = messageWriter.messages.dequeue()
    }
  }

  test("Discards dispatches that have returned and have been partially written") {
    new Ctx {
      override val messageWriter = new MockMessageWriter {
        override def removeForTag(id: Int): DiscardResult = DiscardResult.PartialWrite
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      val RdispatchOk(2, _, `data`) = messageWriter.messages.dequeue()

      tracker.discarded(2, "Foo")
      assert(tracker.lessee.npending == 0)

      val Rdiscarded(2) = messageWriter.messages.dequeue()
    }
  }

  test("Discard dispatches that have been fully written") {
    new Ctx {
      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()
      assert(tracker.lessee.npending == 0)

      val RdispatchOk(2, _, `data`) = messageWriter.messages.dequeue()

      tracker.discarded(2, "Foo")
      assert(tracker.lessee.npending == 0)

      // Nothing to be done: tag already released due to writing the complete response.
      assert(messageWriter.messages.isEmpty)
    }
  }

  test("Discard for dispatch that doesn't exist") {
    new Ctx {
      assert(tracker.currentState == ServerTracker.Open) // touch the lazy to initialize
      assert(tracker.lessee.npending == 0)

      tracker.discarded(2, "Foo")
      assert(tracker.lessee.npending == 0)

      // Nothing to be done: tag wasn't occupied
      assert(messageWriter.messages.isEmpty)
      assert(statsReceiver.counters(Seq("orphaned_tdiscard")) == 1L)
    }
  }

  test("Honors leases") {
    nackOnExpiredLease.let(true) {
      new Ctx {
        // Shut down dispatching
        tracker.lessee.issue(Duration.Zero)
        executor.executeAll()

        val Tlease(0, 0) = messageWriter.messages.dequeue()

        tracker.dispatch(
          Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
        )

        executor.executeAll()
        assert(tracker.lessee.npending == 0)

        val RdispatchNack(2, _) = messageWriter.messages.dequeue()
      }
    }
  }

  test("Drains") {
    new Ctx {
      val serviceP = Promise[Response]()
      val writerDrainP = Promise[Unit]()

      override val service: Service[Request, Response] = Service.const(serviceP)

      override val messageWriter: MockMessageWriter = new MockMessageWriter {
        override def drain(): Future[Unit] = writerDrainP
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      assert(tracker.currentState == ServerTracker.Open)
      assert(tracker.lessee.npending == 1)

      tracker.drain()
      // Still nothing happening.
      assert(tracker.lessee.npending == 1)
      assert(!tracker.drained.isDefined)
      assert(tracker.currentState == ServerTracker.Draining)

      // Further requests are nacked
      tracker.dispatch(
        Tdispatch(tag = 3, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      val RdispatchNack(3, _) = messageWriter.messages.dequeue()

      serviceP.setValue(Response(data))
      executor.executeAll()

      assert(tracker.lessee.npending == 0)
      assert(tracker.currentState == ServerTracker.Closed)
      // Still hasn't flushed the writer
      val RdispatchOk(2, _, `data`) = messageWriter.messages.dequeue()
      assert(!tracker.drained.isDefined)

      // Drain writer
      writerDrainP.setDone()
      assert(tracker.drained.poll == Some(Return.Unit))
    }
  }

  test("Handle tags races") {
    new Ctx {
      val writerDrainP = Promise[Unit]()
      val servicePs = new mutable.Queue[Promise[Response]]

      override val service: Service[Request, Response] = Service.mk { _: Request =>
        val p = Promise[Response]
        servicePs += p
        p
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )
      tracker.discarded(2, "lolz") // cancel it, but the Future still remains
      assert(tracker.lessee.npending() == 0)

      val Rdiscarded(2) = messageWriter.messages.dequeue()

      // Same tag, and now two outstanding promises for the same tag
      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )
      assert(tracker.lessee.npending() == 1)

      val firstP = servicePs.dequeue()
      assert(firstP.isInterrupted.isDefined) // should be interrupted, but we don't care

      val secondP = servicePs.dequeue()
      assert(secondP.isInterrupted.isEmpty) // shouldn't be interrupted

      // Satisfy both promises, should only get a RdispatchOk from the second
      firstP.setValue(Response(Nil, Buf.Empty))
      secondP.setValue(Response(Nil, data))
      executor.executeAll()
      assert(tracker.lessee.npending() == 0)

      assert(messageWriter.messages.size == 1)
      val RdispatchOk(2, _, msg) = messageWriter.messages.dequeue()
      assert(msg == data) // should be the second dispatch
    }
  }

  test("Duplicate tags") {
    new Ctx {
      val writerDrainP = Promise[Unit]()

      override val service: Service[Request, Response] = Service.const(Future.never)

      override val messageWriter: MockMessageWriter = new MockMessageWriter {
        override def drain(): Future[Unit] = writerDrainP
      }

      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )
      tracker.dispatch(
        Tdispatch(tag = 3, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )
      // colliding tag
      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      executor.executeAll()

      assert(tracker.lessee.npending == 0)

      // Waiting for the writer to drain
      assert(!tracker.drained.isDefined)

      assert(messageWriter.messages.size == 2)

      val erredTags = messageWriter.messages.map {
        case Rerr(tag, _) => tag
        case other => sys.error(s"Unexpected type: $other")
      }.toSet

      assert(Set(2, 3) == erredTags)

      messageWriter.messages.clear()

      // Further requests are nacked
      tracker.dispatch(
        Tdispatch(tag = 2, contexts = Nil, dst = Path.empty, dtab = Dtab.empty, req = data)
      )

      val RdispatchNack(2, _) = messageWriter.messages.dequeue()

      executor.executeAll()

      assert(tracker.lessee.npending == 0)
      assert(!tracker.drained.isDefined)

      // Drain writer
      writerDrainP.setDone()
      intercept[IllegalStateException] {
        await(tracker.drained)
      }
    }
  }
}
