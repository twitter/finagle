package com.twitter.finagle.builder

import collection.JavaConversions._

import java.net.InetSocketAddress
import java.util.Collection
import java.util.concurrent.{TimeUnit, Executors}

import org.jboss.netty.channel.socket.nio._

import com.twitter.ostrich
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.stub

object ClientBuilder {
  def apply() = new ClientBuilder
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

// TODO: sampleGranularity, sampleWindow <- rename!

// We're nice to java.
case class ClientBuilder(
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
  _recvBufferSize: Option[Int],
  _exportLoadsToOstrich: Boolean,
  _failureAccrualWindow: Timeout)
{
  import ClientBuilder._
  def this() = this(
    None,                                            // hosts
    None,                                            // codec
    Timeout(Int.MaxValue, TimeUnit.MILLISECONDS),    // connectionTimeout
    Timeout(Int.MaxValue, TimeUnit.MILLISECONDS),    // requestTimeout
    None,                                            // statsReceiver
    Timeout(10, TimeUnit.MINUTES),                   // sampleWindow
    Timeout(10, TimeUnit.SECONDS),                   // sampleGranularity
    None,                                            // name
    None,                                            // hostConnectionLimit
    None,                                            // sendBufferSize
    None,                                            // recvBufferSize
    false,                                           // exportLoadsToOstrich
    Timeout(10, TimeUnit.SECONDS)                    // failureAccrualWindow
  )

  def hosts(hostnamePortCombinations: String) =
    copy(_hosts = Some(parseHosts(hostnamePortCombinations)))

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

  def exportLoadsToOstrich() = copy(_exportLoadsToOstrich = true)

  def failureAccrualWindow(value: Long, unit: TimeUnit) =
    copy(_failureAccrualWindow = Timeout(value, unit))

  // ** BUILDING

  private def bootstrap(codec: Codec)(host: InetSocketAddress) = {
    val bs = new BrokerClientBootstrap(channelFactory)
    bs.setPipelineFactory(codec.clientPipelineFactory)
    bs.setOption("remoteAddress", host)
    bs.setOption("connectTimeoutMillis", _connectionTimeout.duration.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private def pool(limit: Option[Int])(bootstrap: BrokerClientBootstrap) = 
    limit match {
      case Some(limit) =>
        new ConnectionLimitingChannelPool(bootstrap, limit)
      case None =>
        new ChannelPool(bootstrap)
    }

  private def timeout(timeout: Timeout)(broker: Broker) =
    new TimeoutBroker(broker, timeout.value, timeout.unit)

  private def statsRepositoryForLoadedBroker(
    host: InetSocketAddress,
    name: Option[String],
    receiver: Option[StatsReceiver],
    sampleWindow: Timeout,
    sampleGranularity: Timeout) =
  {
    val window      = sampleWindow.duration
    val granularity = sampleGranularity.duration
    if (window < granularity) {
      throw new IncompleteSpecification(
        "window smaller than granularity!")
    }

    val prefix = name map ("%s_".format(_)) getOrElse ""
    val sampleRepository =
      new ObservableSampleRepository[TimeWindowedSample[ScalarSample]] {
        override def makeStat = TimeWindowedSample[ScalarSample](window, granularity)
      }

    for (receiver <- receiver)
      sampleRepository observeTailsWith receiver.observer(prefix, host)

    sampleRepository
  }

  private def failureAccrualBroker(timeout: Timeout)(broker: StatsLoadedBroker) = {
    val window = timeout.duration
    val granularity = Seq((window.inMilliseconds / 10).milliseconds, 1.second).max
    def mk = new LazilyCreatingSampleRepository[TimeWindowedSample[ScalarSample]] {
      override def makeStat = TimeWindowedSample[ScalarSample](window, granularity)
    }
    
    new FailureAccruingLoadedBroker(broker, mk)
  }

  def makeBroker(
    codec: Codec,
    statsRepo: SampleRepository[T forSome { type T <: AddableSample[T] }])
  =
      bootstrap(codec) _                    andThen
      pool(_hostConnectionLimit) _          andThen
      (new PoolingBroker(_))                andThen
      timeout(_requestTimeout) _            andThen
      (new StatsLoadedBroker(_, statsRepo)) andThen
        failureAccrualBroker(_failureAccrualWindow) _

  def build(): Broker = {
    val (hosts, codec) = (_hosts, _codec) match {
      case (None, _) =>
        throw new IncompleteSpecification("No hosts were specified")
      case (_, None) =>
        throw new IncompleteSpecification("No codec was specified")
      case (Some(hosts), Some(codec)) =>
        (hosts, codec)
    }

    val brokers = hosts map { host =>
      val statsRepo = statsRepositoryForLoadedBroker(
        host, _name, _statsReceiver,
        _sampleWindow, _sampleGranularity)

      val broker = makeBroker(codec, statsRepo)(host)

      if (_exportLoadsToOstrich) {
        val hostString = "%s:%d".format(host.getHostName, host.getPort)
        ostrich.Stats.makeGauge(hostString + "_load")   { broker.load   }
        ostrich.Stats.makeGauge(hostString + "_weight") { broker.weight }
      }

      broker
    }

    new LoadBalancedBroker(brokers)
  }

  def buildStub[Request <: AnyRef, Reply <: AnyRef]() =
    new stub.Client[Request, Reply](build())
}
