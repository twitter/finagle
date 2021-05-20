package com.twitter.finagle.netty4.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{ChannelException, Status, UnknownChannelException}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Awaitable, Future, Promise, Return, Throw, Time, Try}
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.{
  ChannelDuplexHandler,
  ChannelHandler,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.channel.embedded.EmbeddedChannel
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class Netty4PushChannelHandleTest extends AnyFunSuite {

  private class NoopSession(handle: PushChannelHandle[Any, Any])
      extends PushSession[Any, Any](handle) {
    val received: mutable.Queue[Any] = new mutable.Queue[Any]()
    def receive(message: Any): Unit = received += message
    def status: Status = handle.status
    def close(deadline: Time): Future[Unit] = handle.close(deadline)
  }

  private case class TestException() extends Exception("boom")

  private def ex: Exception = new TestException()

  def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  private def noopChannel(
    transportHandlers: (String, ChannelHandler)*
  ): (EmbeddedChannel, Netty4PushChannelHandle[Any, Any]) = {
    nettyChannel[Any, Any](transportHandlers: _*) { handle =>
      Future.value(new NoopSession(handle))
    }
  }

  private def nettyChannel[In, Out](
    transportHandlers: (String, ChannelHandler)*
  )(
    f: PushChannelHandle[In, Out] => Future[PushSession[In, Out]]
  ): (EmbeddedChannel, Netty4PushChannelHandle[In, Out]) = {
    val ch = new EmbeddedChannel()
    transportHandlers.foreach { case (name, handler) => ch.pipeline.addLast(name, handler) }
    val (handle, _) = Netty4PushChannelHandle.install[In, Out, PushSession[In, Out]](
      ch,
      _ => (),
      f,
      NullStatsReceiver
    )
    ch -> handle
  }

  test("Satisfies onClose when the channel closes with a Return(())") {
    def testClose(installDriver: Boolean): Unit = {
      @volatile
      var closed: Option[Option[Throwable]] = None
      val (ch, handle) = noopChannel()
      handle.onClose.respond {
        case Return(_) => closed = Some(None)
        case Throw(t) => closed = Some(Some(t))
      }

      if (installDriver) {
        ch.runPendingTasks() // loads the PushSession
        assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
        assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)
      } else {
        assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) == null)
        assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) != null)
      }

      ch.close()
      ch.runPendingTasks()

      assert(closed == Some(None))
      assert(handle.status == Status.Closed)
    }

    testClose(true)
    testClose(false)
  }

  test("close() closes both the Channel and the PushChannelHandle") {
    val (ch, handle) = noopChannel()
    ch.runPendingTasks()
    handle.close()
    ch.runPendingTasks()
    assert(handle.status == Status.Closed)
    assert(!ch.isOpen)
    await(handle.onClose)
  }

  test("Satisfies onClose when the channel throws an Exception") {
    def testException(installDriver: Boolean): Unit = {
      val (ch, handle) = noopChannel()

      if (installDriver) {
        ch.runPendingTasks() // loads the PushSession
        assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
        assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)
      } else {
        assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) == null)
        assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) != null)
      }

      ch.pipeline().fireExceptionCaught(ex)
      ch.runPendingTasks()

      val observed = intercept[UnknownChannelException] {
        await(handle.onClose)
      }

      assert(ex == observed.getCause)
      assert(handle.status == Status.Closed)
      assert(!ch.isOpen)
    }

    testException(true)
    testException(false)
  }

  test(
    "Buffers inbound messages and frees them if the channel is closed before the handler resolves"
  ) {
    val (ch, _) = nettyChannel[Any, Any]()(Function.const(Future.never))
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) == null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) != null)

    val buf = ByteBufAllocator.DEFAULT.heapBuffer(10)
    assert(buf.refCnt() == 1)
    ch.writeInbound(buf)
    ch.close()
    assert(buf.refCnt() == 0)
  }

  test("Buffers inbound messages and gives them to the handler once it resolves") {
    val p = Promise[PushSession[Any, Any]]
    val (ch, handle) = nettyChannel[Any, Any]()(Function.const(p))
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) == null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) != null)

    val buf = ByteBufAllocator.DEFAULT.heapBuffer(10)
    assert(buf.refCnt() == 1)
    ch.writeInbound(buf)

    val s = new NoopSession(handle)
    p.setValue(s)
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)

    val msgs = s.received.dequeueAll(Function.const(true))
    assert(msgs == Seq(buf))
  }

  test("buffer stage is added after ssl stage") {
    val fakeSslStage = new ChannelDuplexHandler
    val (ch, _) = noopChannel("ssl" -> fakeSslStage)
    // since we didn't execute pending tasks we will not have replaced our buffer handler

    def search(lst: List[String]): Unit = lst match {
      case "ssl" :: next :: _ => assert(next == Netty4PushChannelHandle.DelayedByteBufHandler)
      case _ :: next :: tail => search(next :: tail)
      case _ =>
        fail(s"Expected '${Netty4PushChannelHandle.DelayedByteBufHandler}' to be after 'ssl'")
    }

    search(ch.pipeline.iterator.asScala.map(_.getKey).toList)
  }

  test("Writes are propagated out the pipeline") {
    val (ch, handle) = noopChannel()
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)

    // send and forget a single element
    handle.sendAndForget(1)
    ch.runPendingTasks()
    assert(ch.readOutbound[Int]() == 1)

    // send and forget a collection elements
    handle.sendAndForget(Seq(1, 2, 3))
    ch.runPendingTasks()
    assert(ch.readOutbound[Int]() == 1)
    assert(ch.readOutbound[Int]() == 2)
    assert(ch.readOutbound[Int]() == 3)

    @volatile var sendResult: Try[Unit] = null
    // send a single element
    handle.send(1) { sendResult = _ }
    ch.runPendingTasks()
    assert(ch.readOutbound[Int]() == 1)
    assert(sendResult == Return.Unit)

    // send a collection elements
    sendResult = null
    handle.send(Seq(1, 2, 3))(sendResult = _)
    ch.runPendingTasks()
    assert(ch.readOutbound[Int]() == 1)
    assert(ch.readOutbound[Int]() == 2)
    assert(ch.readOutbound[Int]() == 3)
    assert(sendResult == Return.Unit)
  }

  private def failWritePipeline(
    allowToPass: Int
  ): (EmbeddedChannel, Netty4PushChannelHandle[Any, Any]) = {
    val writesFail = new ChannelOutboundHandlerAdapter {
      private[this] var passed = 0
      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit = {
        if (passed < allowToPass) {
          passed += 1
          super.write(ctx, msg, promise)
        } else {
          promise.setFailure(ex)
        }
      }
    }

    val pair @ (ch, _) = noopChannel("writesFail" -> writesFail)
    ch.runPendingTasks()
    pair
  }

  test("write failures are returned to the continuations and fatal") {
    val (ch, handle) = failWritePipeline(0)
    @volatile var sendResult: Try[Unit] = null
    handle.send(1)(sendResult = _)
    ch.runPendingTasks()
    assert(!ch.isOpen)
    assert(handle.status == Status.Closed)
    assert(sendResult == Throw(ChannelException(ex, ch.remoteAddress)))
  }

  test("write failure of multi-writes are returned to the continuations and fatal") {
    (0 until 5).foreach { failAt =>
      val (ch, handle) = failWritePipeline(failAt)
      @volatile var sendResult: Try[Unit] = null
      // Failing a multi-write fails the channel
      handle.send(0 to 5)(sendResult = _)
      ch.runPendingTasks()
      assert(!ch.isOpen)
      assert(handle.status == Status.Closed)
      assert(sendResult == Throw(ChannelException(ex, ch.remoteAddress)))
    }
  }

  test("writeAndForget failures are fatal and close the channel") {
    val (ch, handle) = failWritePipeline(0)
    // Failing a multi-write fails the channel
    handle.sendAndForget(1)
    ch.runPendingTasks()
    assert(!ch.isOpen)
    assert(handle.status == Status.Closed)
    await(handle.onClose.liftToTry)
  }

  test("multi-writeAndForget failures are fatal and close the channel") {
    (0 until 5).foreach { failAt =>
      val (ch, handle) = failWritePipeline(failAt)
      // Failing a multi-write fails the channel
      handle.sendAndForget(0 to 5)
      ch.runPendingTasks()
      assert(!ch.isOpen)
      assert(handle.status == Status.Closed)
      val observed = intercept[UnknownChannelException] {
        await(handle.onClose)
      }

      assert(observed.getCause == ex)
    }
  }

  test("write path is fully formed before session is resolved") {
    val p = Promise[NoopSession]
    val ch = new EmbeddedChannel()

    object OutboundObserver extends ChannelOutboundHandlerAdapter {
      var observedWrite: Any = null

      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit = {
        observedWrite = msg
        super.write(ctx, msg, promise)
      }
    }
    val ObserverName = "observer"

    val protocolInit: ChannelPipeline => Unit = _.addLast(ObserverName, OutboundObserver)
    val (handle, _) =
      Netty4PushChannelHandle.install[Any, Any, NoopSession](
        ch,
        protocolInit,
        _ => p,
        NullStatsReceiver
      )

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) == null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) != null)

    ch.pipeline.names.asScala.toList match {
      case first :: second :: _ =>
        assert(first == Netty4PushChannelHandle.DelayedByteBufHandler && second == ObserverName)

      case other =>
        fail("Unexpected pipeline configuration: " + other)
    }

    val write1 = new Object
    handle.sendAndForget(write1)
    ch.runPendingTasks()

    assert(ch.readOutbound[Any]() == write1)
    assert(OutboundObserver.observedWrite == write1)

    val session1 = new NoopSession(handle)
    p.setValue(session1)
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)

    val write2 = new Object
    handle.sendAndForget(write2)
    ch.runPendingTasks()

    assert(ch.readOutbound[Any]() == write2)
    assert(OutboundObserver.observedWrite == write2)
  }

  test("`registerSession()` replaces the existing session") {
    val p = Promise[NoopSession]
    val (ch, handle) = nettyChannel[Any, Any]()(Function.const(p))
    val session1 = new NoopSession(handle)
    p.setValue(session1)
    ch.runPendingTasks()

    assert(ch.pipeline.get(Netty4PushChannelHandle.SessionDriver) != null)
    assert(ch.pipeline.get(Netty4PushChannelHandle.DelayedByteBufHandler) == null)

    val write1 = new Object
    ch.writeInbound(write1)
    ch.runPendingTasks()
    assert(session1.received.dequeue() == write1)
    val session2 = new NoopSession(handle)
    handle.registerSession(session2)
    val write2 = new Object
    ch.writeInbound(write2)
    ch.runPendingTasks()

    assert(session1.received.isEmpty)
    assert(session2.received.dequeue() == write2)
  }

  test("Uncaught exceptions thrown in the serial executor close the handle") {
    val (ch, handler) = noopChannel()

    assert(handler.status == Status.Open)
    assert(ch.isOpen)

    handler.serialExecutor.execute(new Runnable {
      def run(): Unit = throw new Exception("lol")
    })

    ch.runPendingTasks()

    assert(handler.status == Status.Closed)
    assert(!ch.isOpen)
    assert(handler.onClose.isDefined)
  }
}
