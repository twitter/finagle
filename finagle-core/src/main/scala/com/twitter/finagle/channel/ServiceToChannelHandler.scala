package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import java.util.logging.Level

import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.FutureLatch
import com.twitter.finagle.CodecException
import com.twitter.finagle.Service

class ServiceToChannelHandler[Req, Rep](service: Service[Req, Rep], log: Logger)
  extends ChannelClosingHandler
{
  def this(service: Service[Req, Rep]) = this(service, Logger.getLogger(getClass.getName))

  sealed trait State
  case object Idle extends State
  case object Busy extends State
  case object Draining extends State
  case object Shutdown extends State

  private[this] val state = new AtomicReference[State](Idle)
  private[this] val pendingWrites = new FutureLatch(0)

  private[this] val onShutdownPromise = new Promise[Unit]

  private[this] def shutdown() =
    if (state.getAndSet(Shutdown) != Shutdown) {
      close() onSuccessOrFailure { onShutdownPromise() = Return(()) }
      service.release()
    }

  val onShutdown: Future[Unit] = onShutdownPromise

  // Admit no new requests.
  def drain() = {
    state.set(Draining)
    pendingWrites await { shutdown() }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val channel = ctx.getChannel
    val message = e.getMessage

    pendingWrites.incr()

    var oldState: State = null
    do {
      if (state.compareAndSet(Busy, Busy))
        oldState = Busy
      if (state.compareAndSet(Shutdown, Shutdown))
        oldState = Shutdown
      if (state.compareAndSet(Draining, Draining))
        oldState = Draining

      if (state.compareAndSet(Idle, Busy))
        oldState = Idle
    } while (oldState eq null)

    oldState match {
      case Idle => ()
      case Busy =>
        pendingWrites.decr()
        throw new CodecException("Codec issued concurrent requests")

      case _ =>
        // Let these requests fall on the floor.
        pendingWrites.decr()
        return
    }

    try {
      service(message.asInstanceOf[Req]) respond {
        case Return(value) =>
          state.compareAndSet(Busy, Idle)
          Channels.write(channel, value) onSuccessOrFailure { pendingWrites.decr() }

        case Throw(e: Throwable) =>
          log.log(Level.WARNING, e.getMessage, e)
          shutdown()
      }
    } catch {
      case e: ClassCastException =>
        log.log(
          Level.SEVERE,
          "Got ClassCastException while processing a " +
          "message. This is a codec bug. %s".format(e))

        shutdown()

      case e =>
        Channels.fireExceptionCaught(channel, e)
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    shutdown()
  }

  /**
   * Catch and silence certain closed channel exceptions to avoid spamming
   * the logger.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val cause = e.getCause
    val level = cause match {
      case e: java.nio.channels.ClosedChannelException =>
        Level.FINEST
      case e: java.io.IOException
      if (e.getMessage == "Connection reset by peer" ||
          e.getMessage == "Broken pipe") =>
        Level.FINEST
      case e: Throwable =>
        Level.WARNING
    }

    log.log(
      level, Option(cause.getMessage).getOrElse("Exception caught"), cause)

    shutdown()
  }
}
