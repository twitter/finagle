package com.twitter.finagle.test

import java.net.InetSocketAddress

import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import net.lag.configgy.{Configgy, RuntimeEnvironment}

import com.twitter.ostrich
import com.twitter.finagle.builder._
import com.twitter.finagle.stub._

import com.twitter.util.Future

object ServerTest extends ostrich.Service {
  class Handler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setHeader("Content-Type", "text/plain")
      response.setContent(ChannelBuffers.wrappedBuffer("Mission accomplished".getBytes))
      e.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.load(args)
    val config = Configgy.config

    ostrich.ServiceTracker.register(this)
    ostrich.ServiceTracker.startAdmin(config, runtime)

    val pf = new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline
        pipeline.addLast("handler", new Handler)
        pipeline
      }
    }

    val server = new Stub[HttpRequest, HttpResponse] {
      def call(request: HttpRequest) = Future {
        val response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes))
        response
      }
    }

    ServerBuilder()
     .codec(Http)
     .reportTo(Ostrich())
     .stub(server)
     .bindTo(new InetSocketAddress(10000))
     .build
  }

  def quiesce() = ()
  def shutdown() = ()
}
