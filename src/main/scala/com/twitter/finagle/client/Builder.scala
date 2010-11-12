package com.twitter.finagle.client

import collection.JavaConversions._

import java.net.InetSocketAddress
import java.util.Collection
import java.util.concurrent.{TimeUnit, Executors}

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.http._

import com.twitter.ostrich
import com.twitter.util.TimeConversions._
import com.twitter.util.Duration

import com.twitter.finagle.channel._
import com.twitter.finagle.http.RequestLifecycleSpy
import com.twitter.finagle.thrift.ThriftClientCodec
import com.twitter.finagle.util._
import com.twitter.finagle._

object Http extends Codec {
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

object Thrift extends Codec {
  val pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftCodec", new ThriftClientCodec)
        pipeline
      }
    }
}

object Codec {
  val http = Http
  val thrift = Thrift
}

object Builder {
  def apply() = new Builder
  def get() = apply()

  val channelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

  def parseHosts(hosts: String): java.util.List[InetSocketAddress] = {
    val hostPorts = hosts split Array(' ', ',') filter (_ != "") map (_.split(":"))
    hostPorts map { hp => new InetSocketAddress(hp(0), hp(1).toInt) } toList
  }
}

// We're nice to java.
case class Builder(
  _hosts: Option[Seq[InetSocketAddress]],
  _codec: Option[Codec],
  _connectionTimeout: Timeout,
  _requestTimeout: Timeout,
  _statsReceiver: Option[StatsReceiver],
  _sampleWindow: Timeout,
  _sampleGranularity: Timeout,
  _name: Option[String],
  _hostConnectionLimit: Option[Int],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int])
{
  import Builder._
  def this() = this(
    None,                                                   // hosts
    None,                                                   // codec
    Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),  // connectionTimeout
    Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),  // requestTimeout
    None,                                                   // statsReceiver
    Timeout(10, TimeUnit.MINUTES),                  // sampleWindow
    Timeout(10, TimeUnit.SECONDS),                  // sampleGranularity
    None,                                                   // name
    None,                                                   // hostConnectionLimit
    None,                                                   // sendBufferSize
    None                                                    // recvBufferSize
  )

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

  def sampleWindow(value: Long, unit: TimeUnit) =
    copy(_sampleWindow = Timeout(value, unit))

  def sampleGranularity(value: Long, unit: TimeUnit) =
    copy(_sampleGranularity = Timeout(value, unit))

  def name(value: String) = copy(_name = Some(value))

  def hostConnectionLimit(value: Int) =
    copy(_hostConnectionLimit = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def build() = {
    val (hosts, codec) = (_hosts, _codec) match {
      case (None, _) =>
        throw new IncompleteConfiguration("No hosts were specified")
      case (_, None) =>
        throw new IncompleteConfiguration("No codec was specified")
      case (Some(hosts), Some(codec)) =>
        (hosts, codec)
    }

    val bootstraps = hosts map { host =>
      val bs = new BrokerClientBootstrap(channelFactory)
      bs.setPipelineFactory(codec.pipelineFactory)
      bs.setOption("remoteAddress", host)
      bs.setOption("connectTimeoutMillis", _connectionTimeout.duration.inMilliseconds)
      bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
      // bs.setOption("soLinger", 0)  (TODO)
      bs.setOption("reuseAddress", true)
      _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
      _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
      bs
     }

    val channelPool =
      _hostConnectionLimit map { limit =>
        ((bootstrap: BrokerClientBootstrap) =>
          (new ConnectionLimitingChannelPool(bootstrap, limit)))
      } getOrElse ((new ChannelPool(_)))

    val sampleRepository = new SampleRepository
    val timeoutBrokers = bootstraps map (
     channelPool            andThen
     (new PoolingBroker(_)) andThen
     (new TimeoutBroker(_, _requestTimeout.value, _requestTimeout.unit)))

    // Construct sample stats.
    val granularity = _sampleGranularity.duration
    val window      = _sampleWindow.duration
    if (window < granularity) {
      throw new IncompleteConfiguration(
        "window smaller than granularity!")
    }
    val numBuckets = math.max(1, window.inMilliseconds / granularity.inMilliseconds)
    val statsMaker = () => new TimeWindowedSample[ScalarSample](numBuckets.toInt, granularity)
    val namePrefix = _name map ("%s_".format(_)) getOrElse ""

    val statsBrokers = _statsReceiver match {
      case Some(Ostrich(provider)) =>
        (timeoutBrokers zip hosts) map { case (broker, host) =>
          val prefix = namePrefix
          val suffix = "_%s:%d".format(host.getHostName, host.getPort)
          val samples = new OstrichSampleRepository(prefix, suffix, provider) {
            def makeStats = statsMaker
          }
          new StatsLoadedBroker(broker, samples)
        }

      case _ =>
        timeoutBrokers map { broker =>
          new StatsLoadedBroker(broker, new SampleRepository)
        }
    }

    new LoadBalancedBroker(statsBrokers)
  }

  def buildClient[Request, Reply]() =
    new Client[HttpRequest, HttpResponse](build())
}

