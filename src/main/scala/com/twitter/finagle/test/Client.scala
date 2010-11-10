package com.twitter.finagle.test

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.net.InetSocketAddress

import org.jboss.netty.bootstrap._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.util._
import com.twitter.finagle.http.RequestLifecycleSpy
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.channel._
import com.twitter.finagle.client.Client

import com.twitter.util.{Return, Throw}

object ClientTest {
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
    val bs = new BrokerClientBootstrap(channelFactory)
    bs.setPipelineFactory(theCodecChannelPipelineFactory)
    bs.setOption("remoteAddress", new InetSocketAddress(host, port))
    bs
  }

  def main(args: Array[String]) {
    val endpoints = 0 until 10 map { off => ("localhost", 10000 + off) }
    val bootstraps = endpoints map (makeBootstrap _).tupled
    val brokers = bootstraps map (
      (new ChannelPool(_))                               andThen
      (new PoolingBroker(_))                             andThen
      (new TimeoutBroker(_, 100, TimeUnit.MILLISECONDS)) andThen
      (new StatsLoadedBroker(_)))

    def rewrite(label: String, tree: SampleTree): SampleTree =
      tree match {
        case SampleNode(name, children) =>
          SampleNode(name, children map (rewrite(label, _)))
        case SampleLeaf(name, sample) =>
          SampleNode(name, Seq(SampleLeaf(label, sample)))
      }

    def printStats() {
      val roots = for {
        (broker, (host, port)) <- brokers zip endpoints
        root <- broker.roots
      } yield {
        // Rewrite it so that the host:port is included as well, for
        // rollup.  (We keep this at the leafs).
        rewrite("%s:%d".format(host, port), root)
      }

      for ((_, roots) <- roots groupBy (_.name)) {
        val combined = roots.reduceLeft(_.merge(_))
        println(combined)
      }
    }

    val loadBalanced = new LoadBalancedBroker(brokers)

    val client = new Client[HttpRequest, HttpResponse](loadBalanced)

    for (_ <- 0 until 100)
      makeRequest(client, printStats)
  }

  val count = new AtomicInteger(0)

  def makeRequest(client: Client[HttpRequest, HttpResponse], printer: () => Unit) {
    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond {
      case _ =>
        if (count.incrementAndGet() % 100 == 0)
          printer()
  
        makeRequest(client, printer)
    }
  }
}
