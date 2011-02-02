package com.twitter.finagle.service

import org.specs.Specification

import org.jboss.netty.channel.local._
import org.jboss.netty.channel._
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.handler.codec.http._

import com.twitter.util.TimeConversions._
import com.twitter.util.Throw
import com.twitter.finagle.builder.{
  ClientBuilder, Http, ReferenceCountedChannelFactory}
import com.twitter.finagle.ChannelClosedException

object ClientSpec extends Specification {
  def withServer(handler: ChannelHandler)(spec: ClientBuilder[HttpRequest, HttpResponse] => Unit) {
    val cf = new DefaultLocalServerChannelFactory()

    val bs = new ServerBootstrap(cf)
    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("theHandler", handler)
        pipeline
      }
    })

    val serverAddress = new LocalAddress("server")
    val serverChannel = bs.bind(serverAddress)

    val builder =
      ClientBuilder()
        .channelFactory(new ReferenceCountedChannelFactory(new DefaultLocalClientChannelFactory))
        .hosts(Seq(serverAddress))
        .codec(Http)

    try {
      spec(builder)
    } finally {
      serverChannel.close().awaitUninterruptibly()
    }
  }

  "client service" should {
    var counter = 0
    val closingHandler = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        counter += 1
        Channels.close(ctx.getChannel)
      }
    }

    "report a closed connection when the server doesn't reply" in {
      withServer(closingHandler) { clientBuilder =>
        val client = clientBuilder.build()
        // No failures have happened yet.
        client.isAvailable must beTrue
        val future = client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))
        val resolved = future get(1.second)
        resolved.isThrow must beTrue
        val Throw(cause) = resolved
        cause must haveClass[ChannelClosedException]
      }
    }

    "report a closed connection when the server doesn't reply, without retrying" in {
      withServer(closingHandler) { clientBuilder =>
        val client = clientBuilder
          .retries(10)
          .build()
        val future = client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))
        val resolved = future get(1.second)
        resolved.isThrow must beTrue
        val Throw(cause) = resolved
        cause must haveClass[ChannelClosedException]
        counter must be_==(1)
      }
    }
  }
}
