package com.twitter.finagle.service

import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import java.util.logging.Level

import org.jboss.netty.channel._

import com.twitter.util.{Return, Throw}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.Service

class ServiceToChannelHandler[Req, Rep](service: Service[Req, Rep], log: Logger)
  extends SimpleChannelUpstreamHandler
{
  def this(service: Service[Req, Rep]) = this(service, Logger.getLogger(getClass.getName))
  private[this] val isShutdown = new AtomicBoolean(false)

  private[this] def shutdown(ch: Channel) =
    if (isShutdown.compareAndSet(false, true)) {
      if (ch.isOpen) ch.close()
      service.release()
    }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val channel = ctx.getChannel
    val message = e.getMessage

    try {
      // for an invalid type, the exception would be caught by the
      // SimpleChannelUpstreamHandler.
      val req = message.asInstanceOf[Req]
      service(req) respond {
         case Return(value) =>
           Channels.write(ctx.getChannel, value)

         case Throw(e: Throwable) =>
           log.log(Level.WARNING, e.getMessage, e)
           shutdown(channel)
       }
    } catch {
      case e: ClassCastException =>
        shutdown(channel)
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    shutdown(ctx.getChannel)
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

    shutdown(ctx.getChannel)
  }
}
