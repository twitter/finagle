package com.twitter.finagle.stub

import org.jboss.netty.channel._

import com.twitter.util.{Return, Throw}

import com.twitter.finagle.util.Conversions._

object StubPipelineFactory {
  def apply[Req <: AnyRef, Rep <: AnyRef](stub: Stub[Req, Rep]) = {
    val dispatcher = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val channel = ctx.getChannel
        val message = e.getMessage

        try {
          val req = message.asInstanceOf[Req]

          stub(req) respond {
             case Return(value) =>
               Channels.write(ctx.getChannel, value)
                       .addListener(ChannelFutureListener.CLOSE)

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
        pipeline.addLast("stubDispatcher", dispatcher)
        pipeline
      }
    }
  }
}
