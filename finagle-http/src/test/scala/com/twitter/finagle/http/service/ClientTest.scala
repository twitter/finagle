package com.twitter.finagle.http.service

import org.jboss.netty.channel.local._
import org.jboss.netty.channel._
import org.jboss.netty.bootstrap.ServerBootstrap

import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Throw, Try}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Request, Response, Version, Method}
import com.twitter.finagle.ChannelClosedException

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  def withServer(handler: ChannelHandler)(spec: ClientBuilder.Complete[Request, Response] => Unit) {
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

  var counter = 0
  lazy val closingHandler = new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      counter += 1
      Channels.close(ctx.getChannel)
    }
  }

  test("report a closed connection when the server doesn't reply") {
    withServer(closingHandler) { clientBuilder =>
      counter = 0
      val client = clientBuilder.build()
      try {
        // No failures have happened yet.
        assert(client.isAvailable == true)
        val future = client(Request(Version.Http11, Method.Get, "/"))
        val resolved = Try(Await.result(future, 1.second))
        assert(resolved.isThrow == true)
        val Throw(cause) = resolved
        intercept[ChannelClosedException] {throw cause}
      } finally client.close()
    }
  }

  test("report a closed connection when the server doesn't reply, without retrying") {
    withServer(closingHandler) { clientBuilder =>
      counter = 0
      val client = clientBuilder
        .retries(10)
        .build()
      try {
        val future = client(Request(Version.Http11, Method.Get, "/"))
        val resolved = Try(Await.result(future, 1.second))
        assert(resolved.isThrow == true)
        val Throw(cause) = resolved
        intercept[ChannelClosedException] {throw cause}
        assert(counter == 1)
      } finally client.close()
    }
  }
}
