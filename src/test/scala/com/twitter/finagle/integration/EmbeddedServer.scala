package com.twitter.finagle.integration

import scala.collection.JavaConversions._

import java.net.SocketAddress
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.handler.codec.http._

import com.twitter.ostrich.StatsCollection

import com.twitter.finagle.RandomSocket

object EmbeddedServer {
  def apply() = new EmbeddedServer(RandomSocket())
  val executor = Executors.newCachedThreadPool()
}

class EmbeddedServer(val addr: SocketAddress) {
  import EmbeddedServer._

  // (Publically accessible) stats covering this server.
  val stats = new StatsCollection

  // Server state:
  private[this] var isApplicationNonresponsive = false
  private[this] var isConnectionNonresponsive = false

  private[this] val channels = new DefaultChannelGroup

  private[this] val bootstrap = new ServerBootstrap(
    new NioServerSocketChannelFactory(executor, executor))

  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("decoder", new HttpRequestDecoder)
      pipeline.addLast("encoder", new HttpResponseEncoder)
      pipeline.addLast("logger", new SimpleChannelHandler {
        override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channels.remove(ctx.getChannel)
          stats.incr("closed")
          super.channelClosed(ctx, e)
        }

        override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          if (isConnectionNonresponsive)
            ctx.getChannel.setReadable(false)
          
          channels.add(ctx.getChannel)
          stats.incr("opened")
          super.channelClosed(ctx, e)
        }

        override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
          stats.incr("requests")
          super.messageReceived(ctx, e)
        }

      })
      pipeline.addLast("dots", new SimpleChannelUpstreamHandler {
        override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setHeader("Content-Length", "1")
          response.setContent(ChannelBuffers.wrappedBuffer(".".getBytes))
          if (!isApplicationNonresponsive)
            ctx.getChannel.write(response)
        }
      })
      pipeline
    }
  })

  private[this] var serverChannel = bootstrap.bind(addr)

  def stop() {
    if (serverChannel.isOpen)
      serverChannel.close().awaitUninterruptibly()

    channels.close().awaitUninterruptibly()
    channels.clear()
  }

  def start() {
    if (!serverChannel.isOpen)
      serverChannel = bootstrap.bind(addr)
  }

  def becomeApplicationNonresponsive() {
    isApplicationNonresponsive = true
  }

  def becomeConnectionNonresponsive() {
    isConnectionNonresponsive = true
    channels foreach { channel => channel.setReadable(false) }
  }
}
