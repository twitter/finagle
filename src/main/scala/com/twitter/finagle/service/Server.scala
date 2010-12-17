package com.twitter.finagle.service

import java.util.logging.Logger
import java.util.logging.Level

import org.jboss.netty.channel._

import com.twitter.util.{Return, Throw}

import com.twitter.finagle.util.Conversions._

object ServicePipelineFactory {
  def apply[Req <: AnyRef, Rep <: AnyRef](service: Service[Req, Rep]) = {
    val dispatcher = new SimpleChannelUpstreamHandler {
      private[this] val log = Logger.getLogger(getClass.getName)

      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val channel = ctx.getChannel
        val message = e.getMessage

        try {
          val req = message.asInstanceOf[Req]
          service(req) respond {
             case Return(value) =>
               Channels.write(ctx.getChannel, value)

             case Throw(_) =>
               // TODO: log (invalid reply)
               Channels.close(channel)
           }
        } catch {
          case e: ClassCastException =>
            Channels.close(channel)
        }
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

        log.log(level,
                Option(cause.getMessage).getOrElse("Exception caught"),
                cause)

        ctx.getChannel match {
          case c: Channel
          if c.isOpen =>
            Channels.close(c)
          case _ =>
            ()
        }
      }
    }

    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("serviceDispatcher", dispatcher)
        pipeline
      }
    }
  }
}
