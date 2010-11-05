package com.twitter.finagle.test

import java.util.concurrent.Executors
import java.net.InetSocketAddress

import org.jboss.netty.bootstrap._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.util._
import com.twitter.finagle.http.RequestLifecycleSpy
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.channel._

object Client {
  val channelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

  val theCodecChannelPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }

  def makeBootstrap(host: String, port: Int) = {
    val bs = new ClientBootstrap(channelFactory)
    bs.setPipelineFactory(theCodecChannelPipelineFactory)
    bs.setOption("remoteAddress", new InetSocketAddress(host, port))
    bs
  }

  def main(args: Array[String]) {
    val endpoints = 0 until 10 map { off => ("localhost", 10000 + off) }
    val bootstraps = endpoints map (makeBootstrap _).tupled
    val brokers = bootstraps map (
      (new ChannelPool(_))   andThen
      (new PoolingBroker(_)) andThen
      (new StatsLoadedBroker(_)))

    val loadBalanced = new LoadBalancedBroker(brokers)

    // TODO: Set up stats tree.

    val brokeredBootstrap = new ClientBootstrap(new BrokeredChannelFactory())
    brokeredBootstrap.setPipelineFactory(
      new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast(
            "handler",
            new SimpleChannelUpstreamHandler {
              override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
                println("RECV: %s".format(e.getMessage))
              }
            }
          )
          pipeline
        }
      }
    )
    brokeredBootstrap.setOption("remoteAddress", loadBalanced)

    for (_ <- 0 until 1000000) {
      brokeredBootstrap.connect() {
        case Ok(channel) =>
          val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
          Channels.write(channel, request)
      }
    }
  }
}
