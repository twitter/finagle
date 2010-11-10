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
  def apply() = new Builder

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

class IncompleteClientSpecification(message: String)
  extends Exception(message)

abstract class StatsReceiver

// We're nice to java.
case class Builder(
  _hosts: Option[Seq[InetSocketAddress]],
  _codec: Option[Codec],
  _connectionTimeout: Builder.Timeout,
  _requestTimeout: Builder.Timeout,
  _statsReceiver: Option[StatsReceiver])
{
  import Builder._
  def this() = this(
    None,
    None,
    Builder.Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),
    Builder.Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),
    None)

  def hosts(hostnamePortCombinations: String) =
    copy(_hosts = Some(Builder.parseHosts(hostnamePortCombinations)))

  def hosts(addresses: Collection[InetSocketAddress]) =
    copy(_hosts = Some(addresses toSeq))

  def codec(codec: Codec) =
    copy(_codec = Some(codec))

  def connectionTimeout(value: Long, unit: TimeUnit) =
    copy(_connectionTimeout = Timeout(value, unit))

  def requestTimeout(value: Long, unit: TimeUnit) =
    copy(_requestTimeout = Timeout(value, unit))

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  def build() = {
    val (hosts, codec) = (_hosts, _codec) match {
      case (None, _) =>
        throw new IncompleteClientSpecification("No hosts were specified")
      case (_, None) =>
        throw new IncompleteClientSpecification("No codec was specified")
      case (Some(hosts), Some(codec)) =>
        (hosts, codec)
    }

    val bootstraps = hosts map { host =>
      val bs = new BrokerClientBootstrap(channelFactory)
      bs.setPipelineFactory(codec.pipelineFactory)
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
