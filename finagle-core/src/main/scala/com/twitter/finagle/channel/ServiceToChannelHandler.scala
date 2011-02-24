package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import java.util.logging.Level

import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.AsyncLatch
import com.twitter.finagle.CodecException
import com.twitter.finagle.Service

class ServiceToChannelHandler[Req, Rep](service: Service[Req, Rep], log: Logger)
  extends ChannelClosingHandler
{
  def this(service: Service[Req, Rep]) = this(service, Logger.getLogger(getClass.getName))

  private[this] sealed trait State

  // valid transitions are:
  //
  //    Idle <=> Busy
  //    {Idle, Busy, Draining} => {Draining, Shutdown}
  private[this] case object Idle     extends State
  private[this] case object Busy     extends State
  private[this] case object Draining extends State
  private[this] case object Shutdown extends State

  private[this] val state = new AtomicReference[State](Idle)

  private[this] val onShutdownPromise = new Promise[Unit]

  private[this] def shutdown() =
    if (state.getAndSet(Shutdown) != Shutdown) {
      close() onSuccessOrFailure { onShutdownPromise() = Return(()) }
      service.release()
    }

  /** 
   * onShutdown: this Future is satisfied when the channel has been
   * closed.
   */ 
  val onShutdown: Future[Unit] = onShutdownPromise

  /** 
   * drain(): admit no new requests.
   */ 
  def drain() = {
    var continue = false
    do {
      continue = false
      if      (state.compareAndSet(Idle, Draining)) shutdown()
      else if (state.compareAndSet(Busy, Draining)) ()
      else if (state.get == Shutdown)               ()
      else if (state.get == Draining)               ()
      else continue = true
    } while (continue)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val channel = ctx.getChannel
    val message = e.getMessage

    var oldState: State = null
    do {
      val state_ = state.get
      if (state_ != Idle)
        oldState = state_
      if (state.compareAndSet(Idle, Busy))
        oldState = Idle
    } while (oldState eq null)

    oldState match {
      case Idle => ()
      case Busy => throw new CodecException("Codec issued concurrent requests")
      case _    => /* let these fall on the floor */ return
    }

    try {
      service(message.asInstanceOf[Req]) respond {
        case Return(value) =>
          Channels.write(channel, value) onSuccessOrFailure {
            if (!state.compareAndSet(Busy, Idle) && state.get == Draining)
              shutdown()
          }

        case Throw(e: Throwable) =>
          log.log(Level.WARNING, "service exception", e)
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
