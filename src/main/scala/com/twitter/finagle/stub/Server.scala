package com.twitter.finagle.stub

import org.jboss.netty.channel._

import com.twitter.util.{Return, Throw}

import com.twitter.finagle.util.Conversions._

object StubPipelineFactory {
  def apply[Req <: AnyRef, Rep <: AnyRef](stub: Stub[Req, Rep]) = {
    val dispatcher = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val channel = ctx.getChannel
        e.getMessage match {
         case req: Req =>
           stub(req) respond {
             case Return(value) =>
               Channels.write(ctx.getChannel, value).close()

             case Throw(_) =>
               // TODO: log (invalid reply)
               Channels.close(channel)
           }

         case _ =>
           // TODO: log (invalid request)
           Channels.close(channel)
        }
      }
    }

    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("stubDispatcher", dispatcher)
        pipeline
      }
    }
  }
}
