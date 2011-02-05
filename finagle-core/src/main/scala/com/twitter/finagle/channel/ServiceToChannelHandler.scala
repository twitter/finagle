package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import java.util.logging.Level

import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.Service

class ServiceToChannelHandler[Req, Rep](service: Service[Req, Rep], log: Logger)
  extends ChannelClosingHandler
{
  def this(service: Service[Req, Rep]) = this(service, Logger.getLogger(getClass.getName))

  private[this] val onShutdownPromise = new Promise[Unit]
  @volatile private[this] var isShutdown = false
  @volatile private[this] var isDraining = false
  @volatile private[this] var isIdle = true

  private[this] def shutdown() = synchronized {
    if (!isShutdown) {
      isShutdown = true
      close() onSuccess {
        onShutdownPromise() = Return(())
      }
      service.release()
    }
  }

  val onShutdown: Future[Unit] = onShutdownPromise

  // Admit no new requests.
  def drain() = synchronized {
    isDraining = true

    if (isIdle)
      shutdown()
  }

  // TODO: keep busy state?  today, this is the responsibility of the
  // codec, but this seems icky. as a go-between, we may create a
  // "serialization" handler in the server pipeline.
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = synchronized {
    val channel = ctx.getChannel
    val message = e.getMessage

    if (isDraining) return

    // This is possible if the codec queued messages for us while
    // draining.

    try {
      // for an invalid type, the exception would be caught by the
      // SimpleChannelUpstreamHandler.
      val replyFuture = service(message.asInstanceOf[Req])

      isIdle = false

      // We really want to know when the *write* is done.  That's when
      // we can drain.
      replyFuture respond {
         case Return(value) =>
           // Not really idle actually -- we shouldn't *actually* shut
           // down here..
           
           synchronized { isIdle = true }

           Channels.write(channel, value) onSuccessOrFailure {
             synchronized { if (isDraining) shutdown() }
           }

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
        // XXX: we can probably just disregard all IOException throwables
        Level.FINEST
      case e: Throwable =>
        Level.WARNING
    }

    log.log(
      level, Option(cause.getMessage).getOrElse("Exception caught"), cause)

    shutdown()
  }
}
