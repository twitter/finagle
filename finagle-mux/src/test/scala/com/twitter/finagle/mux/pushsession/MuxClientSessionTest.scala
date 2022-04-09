package com.twitter.finagle.mux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.pushsession.MessageWriter.DiscardResult
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message._
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.Failure
import com.twitter.finagle.Path
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class MuxClientSessionTest extends AnyFunSuite {

  // Expects the Iterable to only contain one message
  private def decode(bufs: Vector[Buf]): Message = {
    if (bufs.length != 1) {
      throw new IllegalStateException(s"Expected a single message, found ${bufs.length}")
    }

    Message.decode(bufs.head)
  }

  private def encode(msg: Message): ByteReader = ByteReader(Message.encode(msg))

  private def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  private abstract class Ctx {
    val name = "CoolService"
    val statsReceiver = new InMemoryStatsReceiver
    val handle = new MockChannelHandle[ByteReader, Buf]()

    def newMessageWriter(handle: PushChannelHandle[_, Buf]): MessageWriter =
      new FragmentingMessageWriter(handle, Int.MaxValue, NullStatsReceiver)

    @volatile
    var failureDetectorStatus: Status = Status.Open

    val session = new MuxClientSession(
      handle = handle,
      h_decoder = new FragmentDecoder(NullStatsReceiver),
      h_messageWriter = newMessageWriter(handle),
      detectorConfig = FailureDetector.MockConfig(() => failureDetectorStatus),
      name = name,
      statsReceiver = statsReceiver
    )

    val service: Service[Request, Response] = await(session.asService)
  }

  test("Propagates the lowest of the PushChannelHandle.status and the FailureDetector.status") {
    new Ctx {
      assert(session.status == Status.Open)

      failureDetectorStatus = Status.Busy
      assert(session.status == Status.Busy)

      failureDetectorStatus = Status.Open
      handle.status = Status.Busy
      assert(session.status == Status.Busy)
    }
  }

  test("Status is Busy if Draining, Closed in Drained") {
    new Ctx {
      assert(session.status == Status.Open)
      service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val Tdispatch(tag, _, _, _, _) = decode(handle.pendingWrites.dequeue().msgs)

      session.receive(encode(Message.Tdrain(1)))
      assert(session.status == Status.Busy) // We have an outstanding dispatch

      session.receive(encode(Message.RdispatchOk(tag, Seq.empty, Buf.Empty)))
      assert(session.status == Status.Closed) // We should be in the Drained state
      assert(!handle.closedCalled)
    }
  }

  test("Performs a Tispatch") {
    new Ctx {
      assert(session.status == Status.Open)
      val respFuture = service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val responseData = Buf.Utf8("Some data")

      val Tdispatch(tag, _, _, _, _) =
        Message.decode(handle.pendingWrites.dequeue().msgs.foldLeft(Buf.Empty)(_.concat(_)))

      session.receive(encode(RdispatchOk(tag, Seq.empty, responseData)))
      handle.serialExecutor
        .executeAll() // Because of the indirection on the first dispatch to check if we can Tdispatch or not

      val resp = await(respFuture)
      assert(resp.body == responseData)
    }
  }

  test("Can downgrade to Treq") {
    new Ctx {
      assert(session.status == Status.Open)
      val respFuture = service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val responseData = Buf.Utf8("Some data")

      val firstWrite = handle.pendingWrites.dequeue()
      firstWrite.completeSuccess()
      val Tdispatch(tag1, _, _, _, _) = decode(firstWrite.msgs)

      session.receive(encode(Rerr(tag1, "Unknown message type")))
      // Because of the indirection on the first dispatch to check if we can Tdispatch or not
      // we end up making a second trip through the executor
      handle.serialExecutor.executeAll()

      val lastWrite = handle.pendingWrites.dequeue()
      lastWrite.completeSuccess()
      val Treq(tag2, _, _) = decode(lastWrite.msgs)

      session.receive(encode(Message.RreqOk(tag2, responseData)))

      val resp = await(respFuture)
      assert(resp.body == responseData)
    }
  }

  test("handles request interrupts") {
    class DiscardCtx(res: MessageWriter.DiscardResult) extends Ctx {
      override def newMessageWriter(handle: PushChannelHandle[_, Buf]): MessageWriter =
        new MessageWriter {
          def removeForTag(id: Int): DiscardResult = res
          // note that we always write the message but spoof the `DiscardResult` for the tests
          def write(message: Message): Unit = handle.sendAndForget(Message.encode(message))

          def drain: Future[Unit] = ???
        }

      assert(session.status == Status.Open)
      val respFuture = service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val responseData = Buf.Utf8("Some data")

      val Tdispatch(tag1, _, _, _, _) = decode(handle.pendingWrites.dequeue().msgs)

      val exc = new Exception("Don't care")
      respFuture.raise(exc)
      handle.serialExecutor.executeAll()

      assert(exc == intercept[Exception] { await(respFuture) })
    }

    // Must have already written it
    new DiscardCtx(DiscardResult.NotFound) {
      val Tdiscarded(tag1prime, _) = decode(handle.pendingWrites.dequeue().msgs)
      assert(tag1 == tag1prime)
    }

    // At least partly written
    new DiscardCtx(DiscardResult.PartialWrite) {
      val Tdiscarded(tag1prime, _) = decode(handle.pendingWrites.dequeue().msgs)
      assert(tag1 == tag1prime)
    }

    // Never sent, so no need to send a Tdiscarded
    new DiscardCtx(DiscardResult.Unwritten) {
      assert(handle.pendingWrites.isEmpty)
    }
  }

  test("Performs PINGs") {
    new Ctx {
      val pingF = session.ping()
      handle.serialExecutor.executeAll()

      assert(!pingF.isDefined)
      assert(decode(handle.pendingWrites.dequeue().msgs) == Tping(Message.Tags.PingTag))

      // Can only have one outstanding ping
      val illegalPingF = session.ping()
      handle.serialExecutor.executeAll()
      intercept[Failure] { await(illegalPingF) }

      assert(!pingF.isDefined)
      session.receive(encode(Rping(Message.Tags.PingTag)))

      assert(await(pingF.liftToTry) == Return(()))
    }
  }

  test("drains requests") {
    new Ctx {
      assert(session.status == Status.Open)
      val respFuture = service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val responseData = Buf.Utf8("Some data")

      val Tdispatch(tag, _, _, _, _) = decode(handle.pendingWrites.dequeue().msgs)

      session.receive(encode(Tdrain(1)))

      assert(session.status == Status.Busy)

      val failedDispatch = service(Request(Path(), Buf.Empty))
      handle.serialExecutor.executeAll()

      val failure = intercept[Failure] { await(failedDispatch) }
      assert(failure.getMessage == "The request was Nacked by the server")

      // Complete the request
      session.receive(encode(RdispatchOk(tag, Seq.empty, responseData)))
      handle.serialExecutor.executeAll()
      assert(await(respFuture).body == responseData)
      assert(session.status == Status.Closed)
      // Even though the status is closed, we don't close the underlying connection
      // automatically and instead wait for a `close()` call on the session.
      assert(!handle.closedCalled)

      val closeF = session.close()
      handle.serialExecutor.executeAll()
      assert(handle.closedCalled)
      assert(handle.onClosePromise.updateIfEmpty(Return.Unit))
      await(closeF)
    }
  }

  test("responds to leases") {
    Time.withCurrentTimeFrozen { ctl =>
      new Ctx {
        assert(session.status == Status.Open)
        assert(session.currentLease == None)
        assert(handle.status == Status.Open)

        session.receive(encode(Message.Tlease(1.millisecond)))
        assert(session.status == Status.Open)
        assert(session.currentLease == Some(1.millisecond))
        assert(handle.status == Status.Open)

        ctl.advance(2.milliseconds)
        assert(session.status == Status.Busy)
        assert(session.currentLease == Some(-1.millisecond))
        assert(handle.status == Status.Open)

        session.receive(encode(Message.Tlease(Message.Tlease.MaxLease)))
        assert(session.currentLease == Some(Message.Tlease.MaxLease))
        assert(session.status === Status.Open)
        assert(handle.status == Status.Open)
      }
    }
  }
}
