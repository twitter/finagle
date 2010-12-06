package com.twitter.finagle.service

import org.jboss.netty.channel._

import com.twitter.util.{Return, Throw}

import com.twitter.finagle.util.Conversions._

object ServicePipelineFactory {
  def apply[Req <: AnyRef, Rep <: AnyRef](service: Service[Req, Rep]) = {
    val dispatcher = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val channel = ctx.getChannel
        val message = e.getMessage

        try {
          val req = message.asInstanceOf[Req]
          service(req) respond {
             case Return(value) =>
               Channels.write(ctx.getChannel, value).close()

             case Throw(_) =>
               // TODO: log (invalid reply)
               Channels.close(channel)
           }
        } catch {
          case e: ClassCastException =>
            Channels.close(channel)
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
