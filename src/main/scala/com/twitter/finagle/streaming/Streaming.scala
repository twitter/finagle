package com.twitter.finagle.streaming

import java.util.concurrent.Executors
import java.net.InetSocketAddress
import java.nio.charset.Charset

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{
  Channels, ChannelPipelineFactory, SimpleChannelUpstreamHandler,
  ChannelHandlerContext, MessageEvent}
import org.jboss.netty.handler.codec.frame.{DelimiterBasedFrameDecoder, Delimiters}

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.http.{
  HttpClientCodec, DefaultHttpRequest, HttpVersion, HttpMethod,
  HttpHeaders, HttpChunk, HttpChunkAggregator}

import com.twitter.finagle.http
import com.twitter.finagle.util.{
  Ok, Error, Cancelled,
  ChannelBufferSnooper, SimpleChannelSnooper}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.channel._

object Streaming {
  val STREAM_HOST = "stream.twitter.com"

  def streamingClientBroker = {
    val socketChannelFactory =
      new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool())

    val bootstrap = new ClientBootstrap
    bootstrap.setFactory(socketChannelFactory)
    bootstrap.setOption("remoteAddress", new InetSocketAddress(STREAM_HOST, 80))
    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        // pipeline.addLast("snooper_", new ChannelBufferSnooper("http"))
        // pipeline.addLast("snooper", new SimpleChannelSnooper("chan"))
        pipeline.addLast("http", new HttpClientCodec)
        pipeline.addLast("lifecycleSpy", http.RequestLifecycleSpy)
        pipeline
      }
    })

    new BootstrapBroker(bootstrap)
  }

  def main(args: Array[String]) {
    val Array(authHeader) = args

    val bootstrap = new ClientBootstrap(new BrokeredChannelFactory)
    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        val delim = Delimiters.lineDelimiter
        val decoder = new DelimiterBasedFrameDecoder(Int.MaxValue, delim(0), delim(1))
        pipeline.addLast("unframer", decoder)
        pipeline.addLast("codec", new StreamingCodec)
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          var count = 0
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            val msg = e.getMessage.asInstanceOf[CachedMessage]
            println("Type: %s; Message: %s".format(Name.forKind(msg.kind), msg.message))
          }
        })

        pipeline
      }
    })

    bootstrap.connect(streamingClientBroker) {
      case Ok(channel) =>
        val request = new DefaultHttpRequest(
          HttpVersion.HTTP_1_1, HttpMethod.GET, "/1/statuses/sample.json")

        request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Basic %s".format(authHeader))
        request.setHeader(HttpHeaders.Names.HOST, STREAM_HOST)

        Channels.write(channel, request)
    }
  }
}
