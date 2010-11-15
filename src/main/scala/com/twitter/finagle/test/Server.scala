package com.twitter.finagle.test

import java.net.InetSocketAddress

import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.server._

object Server {
  def main(args: Array[String]) {

    class Handler extends SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setHeader("Content-Type", "text/plain")
        response.setContent(ChannelBuffers.wrappedBuffer("Mission accomplished".getBytes))
        e.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
      }
    }

    val pf = new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline
        pipeline.addLast("handler", new Handler)
        pipeline
      }
    }
    val bs = Builder().codec(Http)
                      .pipelineFactory(pf)
                      .build

    val addr = new InetSocketAddress(8888)
    println("HTTP demo running on %s".format(addr))
    bs.bind(addr)
  }
}
