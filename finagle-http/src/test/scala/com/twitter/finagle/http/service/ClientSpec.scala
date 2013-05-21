package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit

import org.jboss.netty.channel.local._
import org.jboss.netty.channel._
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.handler.codec.http._

import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Throw, Try}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.ChannelClosedException

class ClientSpec extends SpecificationWithJUnit {
  def withServer(handler: ChannelHandler)(spec: ClientBuilder.Complete[HttpRequest, HttpResponse] => Unit) {
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
        .channelFactory(new DefaultLocalClientChannelFactory)
        .hosts(Seq(serverAddress))
        .hostConnectionLimit(1)
        .codec(Http())

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
        val resolved = Try(Await.result(future, 1.second))
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
        val resolved = Try(Await.result(future, 1.second))
        resolved.isThrow must beTrue
        val Throw(cause) = resolved
        cause must haveClass[ChannelClosedException]
        counter must be_==(1)
      }
    }
  }
}
