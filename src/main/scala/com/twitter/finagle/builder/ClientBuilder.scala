package com.twitter.finagle.builder

import collection.JavaConversions._

import java.net.{SocketAddress, InetSocketAddress}
import java.util.Collection
import java.util.logging.Logger
import java.util.concurrent.{TimeUnit, Executors}

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._

import com.twitter.ostrich
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.service

object ClientBuilder {
  def apply() = new ClientBuilder
  def get() = apply()

  val defaultChannelFactory =
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
  _hosts: Option[Seq[SocketAddress]],
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
  _failureAccrualWindow: Timeout,
  _retries: Option[Int],
  _initialBackoff: Option[Duration],
  _backoffMultiplier: Option[Int],
  _logger: Option[Logger],
  _channelFactory: Option[ChannelFactory],
  _proactivelyConnect: Option[Duration])
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
    Timeout(10, TimeUnit.SECONDS),                   // failureAccrualWindow
    None,                                            // retries
    None,                                            // initialBackoff
    None,                                            // backoffMultiplier
    None,                                            // logger
    None,                                            // channelFactory
    None                                             // proactivelyConnect
  )

  def hosts(hostnamePortCombinations: String): ClientBuilder =
    copy(_hosts = Some(parseHosts(hostnamePortCombinations)))

  def hosts(addresses: Collection[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def hosts(addresses: Iterable[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def codec(codec: Codec): ClientBuilder =
    copy(_codec = Some(codec))

  def connectionTimeout(value: Long, unit: TimeUnit): ClientBuilder =
    copy(_connectionTimeout = Timeout(value, unit))

  def connectionTimeout(duration: Duration): ClientBuilder =
    copy(_connectionTimeout = Timeout(duration.inMillis, TimeUnit.MILLISECONDS))

  def requestTimeout(value: Long, unit: TimeUnit): ClientBuilder =
    copy(_requestTimeout = Timeout(value, unit))

  def requestTimeout(duration: Duration): ClientBuilder =
    copy(_requestTimeout = Timeout(duration.inMillis, TimeUnit.MILLISECONDS))

  def reportTo(receiver: StatsReceiver): ClientBuilder =
    copy(_statsReceiver = Some(receiver))

  def sampleWindow(value: Long, unit: TimeUnit): ClientBuilder =
    copy(_sampleWindow = Timeout(value, unit))

  def sampleGranularity(value: Long, unit: TimeUnit): ClientBuilder =
    copy(_sampleGranularity = Timeout(value, unit))

  def name(value: String): ClientBuilder = copy(_name = Some(value))

  def hostConnectionLimit(value: Int): ClientBuilder =
    copy(_hostConnectionLimit = Some(value))

  def retries(value: Int): ClientBuilder =
    copy(_retries = Some(value))

  def initialBackoff(value: Duration): ClientBuilder =
    copy(_initialBackoff = Some(value))

  def backoffMultiplier(value: Int): ClientBuilder =
    copy(_backoffMultiplier = Some(value))

  def sendBufferSize(value: Int): ClientBuilder = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int): ClientBuilder = copy(_recvBufferSize = Some(value))

  def exportLoadsToOstrich(): ClientBuilder = copy(_exportLoadsToOstrich = true)

  def failureAccrualWindow(value: Long, unit: TimeUnit): ClientBuilder =
    copy(_failureAccrualWindow = Timeout(value, unit))

  def channelFactory(cf: ChannelFactory): ClientBuilder =
    copy(_channelFactory = Some(cf))

  def proactivelyConnect(duration: Duration): ClientBuilder =
    copy(_proactivelyConnect = Some(duration))

  // ** BUILDING
  def logger(logger: Logger): ClientBuilder = copy(_logger = Some(logger))

  private def bootstrap(codec: Codec)(host: SocketAddress) = {
    val bs = new BrokerClientBootstrap(_channelFactory getOrElse defaultChannelFactory)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.clientPipelineFactory.getPipeline
        for (logger <- _logger) {
          pipeline.addFirst(
            "channelSnooper",
            ChannelSnooper(_name getOrElse "client")(logger.info))
        }

        pipeline
      }
    }
    bs.setPipelineFactory(pf)
    bs.setOption("remoteAddress", host)
    bs.setOption("connectTimeoutMillis", _connectionTimeout.duration.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private def pool(limit: Option[Int], proactivelyConnect: Option[Duration])
                  (bootstrap: BrokerClientBootstrap) =
    limit match {
      case Some(limit) =>
        new ConnectionLimitingChannelPool(bootstrap, limit, proactivelyConnect)
      case None =>
        new ChannelPool(bootstrap, proactivelyConnect)
    }

  private def timeout(timeout: Timeout)(broker: Broker) =
    new TimeoutBroker(broker, timeout.value, timeout.unit)

  private def retrying(broker: Broker) =
    (_retries, _initialBackoff, _backoffMultiplier) match {
      case (Some(retries: Int), None, None) =>
        new RetryingBroker(broker, retries)
      case (Some(retries: Int), Some(backoff: Duration), Some(multiplier: Int)) =>
        new ExponentialBackoffRetryingBroker(broker, backoff, multiplier)
      case (_, _, _) =>
        broker
    }

  private def statsRepositoryForLoadedBroker(
    sockAddr: SocketAddress,
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
      sampleRepository observeTailsWith receiver.observer(prefix, sockAddr toString)

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
    statsRepo: SampleRepository[T forSome { type T <: AddableSample[T] }]) =
      bootstrap(codec) _                                andThen
      pool(_hostConnectionLimit, _proactivelyConnect) _ andThen
      (new PoolingBroker(_))                            andThen
      timeout(_requestTimeout) _                        andThen
      (new StatsLoadedBroker(_, statsRepo))             andThen
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
        val hostString = host.toString
        ostrich.Stats.makeGauge(hostString + "_load")      { broker.load   }
        ostrich.Stats.makeGauge(hostString + "_weight")    { broker.weight }
        ostrich.Stats.makeGauge(hostString + "_available") { if (broker.isAvailable) 1 else 0 }
      }

      broker
    }

    retrying(new LoadBalancedBroker(brokers))
  }

  def buildService[Request <: AnyRef, Reply <: AnyRef]() =
    new service.Client[Request, Reply](build())
}
