package com.twitter.finagle.netty4.pushsession

import com.twitter.finagle.{
  ChannelClosedException,
  ChannelException,
  Status,
  UnknownChannelException
}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo, UsingSslSessionInfo}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util._
import io.netty.buffer.ByteBuf
import io.netty.channel.{
  Channel,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelPipeline,
  EventLoop
}
import io.netty.handler.ssl.SslHandler
import io.netty.util
import io.netty.util.concurrent.GenericFutureListener
import java.net.SocketAddress
import java.util.concurrent.Executor
import scala.util.control.NonFatal

/**
 * Netty 4 implementation of the [[PushChannelHandle]]
 *
 * It is assumed that this stage is added after any necessary layer 4 handshakes such
 * as TLS negotiation as it will set the channel auto-read to `false`. If stages before
 * it require data after this channel has been installed but before it is ready they need
 * to manage their read interests accordingly. After the [[PushSession]] is installed
 * auto-read is set to `true` and this implementation won't request or modify the read
 * interests for the remainder of its lifecycle.
 *
 * @see [[com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer]] for the tools
 *     used to initialize TLS and delay the connect promise until negotiation is complete.
 */
private final class Netty4PushChannelHandle[In, Out] private (
  ch: Channel,
  statsReceiver: StatsReceiver)
    extends PushChannelHandle[In, Out] {

  import Netty4PushChannelHandle._

  // A runnable that catches unhandled exceptions and closes the handle
  // instead of letting netty swallow them.
  private[this] abstract class SafeRunnable extends Runnable {
    // task to execute. Uncaught exceptions result in a log message and closure of the handle.
    def tryRun(): Unit

    final def run(): Unit = {
      try tryRun()
      catch {
        case NonFatal(t) =>
          log.error(
            t,
            "Unhandled exception detected in push-session serial executor. Shutting down."
          )
          statsReceiver.counter("push", "unhandled_exceptions", t.getClass.getName).incr()
          // This is happening in the channels event loop so we're thread safe.
          handleFail(Throw(t))
      }
    }
  }

  private[this] final class SafeExecutor(eventLoop: EventLoop) extends Executor {
    private[this] final class SafeProxy(underlying: Runnable) extends SafeRunnable {
      def tryRun(): Unit = underlying.run()
    }

    def safeExecute(command: SafeRunnable): Unit = eventLoop.execute(command)
    def execute(command: Runnable): Unit = safeExecute(new SafeProxy(command))
  }

  @volatile
  private[this] var failed: Boolean = false
  private[this] val closePromise = Promise[Unit]()
  private[this] val safeExecutor = new SafeExecutor(ch.eventLoop)

  def serialExecutor: Executor = safeExecutor

  def onClose: Future[Unit] = closePromise

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress = ch.remoteAddress

  // This is a `def` in order to avoid memoizing this value eagerly (and incorrectly)
  // when doing OppTls.
  def sslSessionInfo: SslSessionInfo =
    ch.pipeline.get(classOf[SslHandler]) match {
      case null => NullSslSessionInfo
      case handler =>
        try {
          new UsingSslSessionInfo(handler.engine.getSession)
        } catch {
          case NonFatal(_) => NullSslSessionInfo
        }
    }

  def status: Status =
    if (failed || !ch.isOpen) Status.Closed
    else Status.Open

  // All send methods need to schedule their writes in the executor to guarantee
  // that they are ordered even if the call to send doesn't originate from the
  // serial executor itself. This is because Netty will fast track writes that
  // happen to be on the right thread and defer others by scheduling them. The
  // only way to make sure it's totally fair is to push everyone through the
  // Executor regardless of the thread calling send.
  def send(messages: Iterable[Out])(continuation: (Try[Unit]) => Unit): Unit = {
    if (messages.isEmpty) {
      // We schedule it with the executor so as to satisfy the guarantee that the
      // continuation will be run later.
      safeExecutor.safeExecute(new SafeRunnable {
        def tryRun(): Unit = continuation(Return.Unit)
      })
    } else
      safeExecutor.safeExecute(new SafeRunnable {
        def tryRun(): Unit = {
          // When doing a batch write we only have a single handle to all writes, so if
          // one fails there is no way to say which one and there is no reason to continue.
          // Therefore, we let all but the last use the voidPromise which will fail the
          // whole channel if they fail and send the last one with a normal promise.
          val it = messages.iterator

          // We cache one message so that we can flush the last element
          var next = it.next()
          while (it.hasNext) {
            ch.write(next, ch.voidPromise())
            next = it.next()
          }

          // The standard flush behavior will suffice for the last message
          handleWriteAndFlush(next, continuation)
        }
      })
  }

  // See note above about the scheduling of send messages
  def send(message: Out)(continuation: (Try[Unit]) => Unit): Unit = {
    safeExecutor.safeExecute(new SafeRunnable {
      def tryRun(): Unit = handleWriteAndFlush(message, continuation)
    })
  }

  // See note above about the scheduling of send messages
  def sendAndForget(message: Out): Unit =
    safeExecutor.safeExecute(new SafeRunnable {
      def tryRun(): Unit = ch.writeAndFlush(message, ch.voidPromise())
    })

  // See note above about the scheduling of send messages
  def sendAndForget(messages: Iterable[Out]): Unit =
    if (messages.nonEmpty) safeExecutor.safeExecute(new SafeRunnable {
      def tryRun(): Unit = {
        val it = messages.iterator
        // Cache one element in `next` so we can flush the last one
        var next = it.next()
        while (it.hasNext) {
          ch.write(next, ch.voidPromise())
          next = it.next()
        }

        ch.writeAndFlush(next, ch.voidPromise())
      }
    })

  // We don't have any scarce resources that need some deadline to cleanup
  // so we just clean everything up now.
  // onClose should handle exceptions and not surface those.
  def close(deadline: Time): Future[Unit] = {
    if (ch.isOpen) ch.close()
    onClose.transform(_ => Future.Done)
  }

  def registerSession(newSession: PushSession[In, Out]): Unit = {
    ch.pipeline.get(classOf[SessionDriver]) match {
      case null =>
        throw new IllegalStateException(
          "Tried to replace the PushSession when the previous session hasn't been installed"
        )
      case driver => driver.registerSession(newSession)
    }
  }

  // It is expected that this will be executed from a task explicitly scheduled in
  // the serial executor which means that it was run 'later' from whatever action on
  // the Netty4PushTransport scheduled it since scheduling shouldn't result in the
  // task being run before the `.execute` method returns.
  private[this] def handleWriteAndFlush(message: Out, continuation: Try[Unit] => Unit): Unit = {
    val op = ch.writeAndFlush(message)
    if (op.isSuccess) continuation(Return.Unit)
    else
      op.addListener(new GenericFutureListener[util.concurrent.Future[Void]] {
        def operationComplete(future: util.concurrent.Future[Void]): Unit = {
          val result =
            if (future.isSuccess) Return.Unit
            else {
              val exc = toCause(future.cause)
              handleFail(exc)
              exc
            }

          continuation(result)
        }
      })
  }

  // Bounce the call to `handleFail` through the executor to ensure that it happens 'later'
  private[this] def scheduleFailure(cause: Try[Unit]): Unit = {
    safeExecutor.safeExecute(new SafeRunnable {
      def tryRun(): Unit = handleFail(cause)
    })
  }

  // Must be called from within the serialExecutor
  private def handleFail(cause: Try[Unit]): Unit = {
    if (!failed) {
      failed = true
      // We trampoline the satisfaction of the close promise to make sure
      // users don't get inadvertent re-entrance due to the continuations
      // attached to the promise potentially being run right away.
      safeExecutor.safeExecute(new SafeRunnable {
        def tryRun(): Unit = closePromise.updateIfEmpty(cause)
      })

      close()
    }
  }

  private[this] def handleChannelExceptionCaught(exc: Throwable): Unit = {
    // We make sure these events are trampolined through the serial executor
    // to guard against re-entrance.
    scheduleFailure(toCause(exc))
  }

  private[this] def toCause(ex: Throwable): Try[Unit] =
    ChannelException(ex, remoteAddress) match {
      case _: ChannelClosedException =>
        Return.Unit // These are considered 'normal'
      case unknown: UnknownChannelException =>
        // We got an exception we're not prepared for, so lets log and count it.
        log.info(ex, "Unknown exception closed channel.")
        statsReceiver.counter("push", "unknown_exceptions", ex.getClass.getName).incr()
        Throw(unknown)
      case other =>
        Throw(other)
    }

  private[this] def handleChannelInactive(): Unit = {
    // We make sure these events are trampolined through the serial executor
    // to guard against re-entrance.
    scheduleFailure(Return.Unit)
  }

  private[this] final class SessionDriver(@volatile private var session: PushSession[In, Out])
      extends ChannelInboundHandlerAdapter {

    // in service of the `PushChannelHandle.registerSession` method
    def registerSession(newSession: PushSession[In, Out]): Unit = {
      session = newSession
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      val m = msg.asInstanceOf[In]
      safeExecutor.safeExecute(new SafeRunnable {
        def tryRun(): Unit = session.receive(m)
      })
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit =
      handleChannelInactive()

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit =
      handleChannelExceptionCaught(e)
  }

  // This is a helper ChannelHandler to guard against the socket sending messages to the
  // session before its ready. In the common case this likely doesn't actually catch any
  // messages but is here for corner cases such as a chunk of data following right after
  // the TLS handshake and before we've had a chance to turn auto-read off.
  private final class DelayedByteBufHandler extends ChannelInboundHandlerAdapter {
    // 8 is the minimum initial size allowed by the ArrayDeque implementation
    private[this] val pendingMessages = new java.util.ArrayDeque[ByteBuf](8)

    // Because `Netty4PushChannelHandle.install` is called from within the channels
    // `EventLoop`, we should be populating this field eagerly, during the call to
    // Channel.pipeline.addLast(DelayedByteBufHandler, thisInstance) in
    // `Netty4PushChannelHandle.install`.
    private[this] var ctx: ChannelHandlerContext = null

    def installedInPipeline: Boolean = {
      ctx != null && !ctx.isRemoved
    }

    /** Removes itself from the pipeline and sends its messages */
    def installSessionDriver(session: PushSession[In, Out]): Unit = {
      if (!ctx.isRemoved) {
        ch.pipeline.addLast(SessionDriver, new SessionDriver(session))
        ch.pipeline.remove(this)
        ch.config.setAutoRead(true)

        // Empty our queue
        while (!pendingMessages.isEmpty) {
          ctx.fireChannelRead(pendingMessages.poll())
        }
      } else if (!pendingMessages.isEmpty) {
        // The pipeline must have closed on us and should
        // have been drained on `channelInactive`, but wasn't.
        // This should never happen, so fail aggressively.
        val channelStateMsg =
          "DelayStage has been removed from the pipeline but not drained: " +
            s"Channel(isOpen: ${ch.isOpen}, isActive: ${ch.isActive})"

        val exc = new IllegalStateException(channelStateMsg)
        log.error(exc, channelStateMsg)

        drainQueue()
        handleFail(Throw(exc))
      }
    }

    override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
      this.ctx = ctx
      ch.config.setAutoRead(false)
    }

    // Need to make sure to release anything that is still laying around
    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      drainQueue()
      handleChannelInactive()
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
      case bb: ByteBuf =>
        pendingMessages.add(bb)
      case other =>
        val ex = new IllegalStateException(s"Received unexpected message: $other")
        ctx.fireExceptionCaught(ex)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
      handleChannelExceptionCaught(e)
    }

    private[this] def drainQueue(): Unit = {
      while (!pendingMessages.isEmpty) {
        pendingMessages.poll().release()
      }
    }
  }

  override def toString: String = s"Netty4PushChannelHandle($ch)"
}

private object Netty4PushChannelHandle {
  private val log: Logger = Logger.get

  val SessionDriver: String = "pushSessionDriver"
  val DelayedByteBufHandler: String = "delayedByteBufHandler"

  /**
   * Install a [[Netty4PushChannelHandle]] in the pipeline and wire up lifetime events
   *
   * Protocol related netty pipeline initialization is deferred until the session has
   * resolved. The resultant `Future[T]` will resolve once the session has been installed
   * and the pipeline is receiving events from the socket.
   *
   * @note This must be called from within the `EventLoop` this `Channel` belongs to to
   *       avoid racy behavior in shutdown logic since adding handlers to a closed
   *       `Channel` won't receive channelInactive events and we would otherwise have to
   *       handle race conditions between `ChannelHandlerContext` instances resolving and
   *       session resolution.
   *
   * @throws IllegalStateException if this method is called from outside of the Netty4
   *                               `EventLoop` that this `Channel` belongs to.
   */
  def install[In, Out, T <: PushSession[In, Out]](
    ch: Channel,
    protocolInit: ChannelPipeline => Unit,
    sessionFactory: PushChannelHandle[In, Out] => Future[T],
    statsReceiver: StatsReceiver
  ): (Netty4PushChannelHandle[In, Out], Future[T]) = {
    if (!ch.eventLoop.inEventLoop) {
      throw new IllegalStateException(
        s"Expected to be called from within the `Channel`s " +
          s"associated `EventLoop` (${ch.eventLoop}), instead called " +
          s"from thread ${Thread.currentThread}"
      )
    }

    val p = Promise[T]
    p.setInterruptHandler { case _ => ch.close() }

    val channelHandle = new Netty4PushChannelHandle[In, Out](ch, statsReceiver)
    val delayStage = new channelHandle.DelayedByteBufHandler
    ch.pipeline.addLast(DelayedByteBufHandler, delayStage)

    // This should've happened within the call to `.addLast` since we're
    // running from inside the `EventLoop` the `Channel` belongs to.
    assert(delayStage.installedInPipeline)

    // We initialize the protocol level stuff after adding the delay stage
    // so that the write path is fully formed but the delay stage is in
    // a position that we know will be dealing in ByteBuf's.
    protocolInit(ch.pipeline)

    // Link resolution of the PushSession to installing the rest of the pipeline
    sessionFactory(channelHandle).respond { result =>
      ch.eventLoop.execute(new Runnable {
        def run(): Unit = {
          result match {
            case Return(session) =>
              delayStage.installSessionDriver(session)

            case t @ Throw(_) =>
              // make sure we clean up associated resources
              channelHandle.handleFail(t.cast[Unit])
          }

          p.updateIfEmpty(result)
        }
      })
    }

    channelHandle -> p
  }
}
