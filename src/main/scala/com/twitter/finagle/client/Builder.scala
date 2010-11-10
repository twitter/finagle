package com.twitter.finagle.client

import collection.JavaConversions._

import java.net.InetSocketAddress
import java.util.Collection
import java.util.concurrent.{TimeUnit, Executors}

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.channel._
import com.twitter.finagle.http.RequestLifecycleSpy
import com.twitter.finagle.thrift.ThriftClientCodec

sealed abstract class Codec {
  val pipelineFactory: ChannelPipelineFactory
}

case object Http extends Codec {
  val pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }
}

case object Thrift extends Codec {
  val pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftCodec", new ThriftClientCodec)
        pipeline
      }
    }
}

object Builder {
  val channelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

  case class Timeout(value: Long, unit: TimeUnit)

  def parseHosts(hosts: String): java.util.List[InetSocketAddress] = {
    val hostPorts = hosts split Array(' ', ',') filter (_ != "") map (_.split(":"))
    hostPorts map { hp => new InetSocketAddress(hp(0), hp(1).toInt) } toList
  }
}

// We're nice to java.
case class Builder(
  _hosts: Seq[InetSocketAddress],
  _codec: Codec,
  _connectionTimeout: Builder.Timeout,
  _requestTimeout: Builder.Timeout)
{
  import Builder._

  def this(hosts: Collection[InetSocketAddress], codec: Codec) = this(
    hosts toSeq,
    codec,
    Builder.Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),
    Builder.Timeout(Long.MaxValue, TimeUnit.MILLISECONDS))

  def this(hosts: String, codec: Codec) =
    this(Builder.parseHosts(hosts), codec)

  def connectionTimeout(value: Long, unit: TimeUnit) =
    copy(_connectionTimeout = Timeout(value, unit))

  def requestTimeout(value: Long, unit: TimeUnit) =
    copy(_requestTimeout = Timeout(value, unit))

  // TODO: reportTo(...)

  def build() = {
    val bootstraps = _hosts map { host =>
      val bs = new BrokerClientBootstrap(channelFactory)
      bs.setPipelineFactory(_codec.pipelineFactory)
      bs.setOption("remoteAddress", host)
      bs
     }

    val brokers = bootstraps map (
     (new ChannelPool(_))        andThen
     (new PoolingBroker(_))      andThen
     (new TimeoutBroker(
       _, _connectionTimeout.value,
       _connectionTimeout.unit)) andThen
     (new StatsLoadedBroker(_)))    

    new LoadBalancedBroker(brokers)
  }
}
