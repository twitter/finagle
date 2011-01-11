package com.twitter.finagle.builder

import collection.JavaConversions._

import java.net.{SocketAddress, InetSocketAddress}
import java.util.Collection
import java.util.logging.Logger
import java.util.concurrent.Executors

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._

import com.twitter.util.{Duration, JavaTimer}
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.service
import com.twitter.finagle.stats.{StatsRepository, TimeWindowedStatsRepository, StatsReceiver}

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

/**
 * A word about the default values:
 *
 *   o connectionTimeout: optimized for within a datanceter
 *   o by default, no request timeout
 */
case class ClientBuilder(
  _hosts: Option[Seq[SocketAddress]],
  _codec: Option[Codec],
  _connectionTimeout: Duration,
  _requestTimeout: Duration,
  _statsReceiver: Option[StatsReceiver],
  _loadStatistics: (Int, Duration),
  _failureAccrualStatistics: (Int, Duration),
  _name: Option[String],
  _hostConnectionLimit: Option[Int],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _retries: Option[Int],
  _initialBackoff: Option[Duration],
  _backoffMultiplier: Option[Int],
  _logger: Option[Logger],
  _channelFactory: Option[ChannelFactory],
  _proactivelyConnect: Option[Duration])
{
  import ClientBuilder._
  def this() = this(
    None,                // hosts
    None,                // codec
    10.milliseconds,     // connectionTimeout
    Duration.MaxValue,   // requestTimeout
    None,                // statsReceiver
    (60, 10.seconds),    // loadStatistics
    (10, 1.second),      // failureAccrualStatistics
    None,                // name
    None,                // hostConnectionLimit
    None,                // sendBufferSize
    None,                // recvBufferSize
    None,                // retries
    None,                // initialBackoff
    None,                // backoffMultiplier
    None,                // logger
    None,                // channelFactory
    None                 // proactivelyConnect
  )

  def hosts(hostnamePortCombinations: String): ClientBuilder =
    copy(_hosts = Some(parseHosts(hostnamePortCombinations)))

  def hosts(addresses: Collection[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def hosts(addresses: Iterable[SocketAddress]): ClientBuilder =
    copy(_hosts = Some(addresses toSeq))

  def codec(codec: Codec): ClientBuilder =
    copy(_codec = Some(codec))

  def connectionTimeout(duration: Duration): ClientBuilder =
    copy(_connectionTimeout = duration)

  def requestTimeout(duration: Duration): ClientBuilder =
    copy(_requestTimeout = duration)

  def reportTo(receiver: StatsReceiver): ClientBuilder =
    copy(_statsReceiver = Some(receiver))

  /**
   * The interval over which to aggregate load statistics.
   */
  def loadStatistics(numIntervals: Int, interval: Duration): ClientBuilder = {
    require(numIntervals >= 1, "Must have at least 1 window to sample statistics over")

    copy(_loadStatistics = (numIntervals, interval))
  }

  /**
   * The interval over which to aggregate failure accrual statistics.
   */
  def failureAccrualStatistics(numIntervals: Int, interval: Duration): ClientBuilder = {
    require(numIntervals >= 1, "Must have at least 1 window to sample statistics over")

    copy(_failureAccrualStatistics = (numIntervals, interval))
  }

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
    bs.setOption("connectTimeoutMillis", _connectionTimeout.inMilliseconds)
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

  private def timeout(timeout: Duration)(broker: Broker) =
    new TimeoutBroker(broker, timeout)

  private def retrying(broker: Broker) =
    (_retries, _initialBackoff, _backoffMultiplier) match {
      case (Some(retries: Int), None, None) =>
        RetryingBroker.tries(broker, retries)
      case (Some(retries: Int), Some(backoff: Duration), Some(multiplier: Int)) =>
        RetryingBroker.exponential(broker, backoff, multiplier)
      case (_, _, _) =>
        broker
    }

  def makeBroker(
    codec: Codec,
    loadStatsRepository: StatsRepository,
    failureAccruingStatsRepo: StatsRepository) =
      bootstrap(codec) _                                andThen
      pool(_hostConnectionLimit, _proactivelyConnect) _ andThen
      (new PoolingBroker(_))                            andThen
      timeout(_requestTimeout) _                        andThen
      (new StatsLoadedBroker(_, loadStatsRepository))   andThen
      (new FailureAccruingLoadedBroker(_, failureAccruingStatsRepo))

  def build(): Broker = {
    val (hosts, codec) = (_hosts, _codec) match {
      case (None, _) =>
        throw new IncompleteSpecification("No hosts were specified")
      case (Some(hosts), _) if hosts.length == 0 =>
        throw new IncompleteSpecification("Empty host list was specified")
      case (_, None) =>
        throw new IncompleteSpecification("No codec was specified")
      case (Some(hosts), Some(codec)) =>
        (hosts, codec)
    }

    val timer = new JavaTimer
    val brokers = hosts map { host =>
      val statsRepository = {
        val statsRepository = new TimeWindowedStatsRepository(
          _loadStatistics._1, _loadStatistics._2, timer)
        statsRepository.scope(
          "service" -> _name.getOrElse(""),
          "host" -> host.toString)
      }

      val failureAccruingStatsRepo = {
        new TimeWindowedStatsRepository(_failureAccrualStatistics._1, _failureAccrualStatistics._2)
      }
      val broker = makeBroker(codec, statsRepository, failureAccruingStatsRepo)(host)

      _statsReceiver.foreach { statsReceiver =>
        val hostString = host.toString
        statsReceiver.mkGauge("host" -> hostString, "load" -> "broker", broker.load)
        statsReceiver.mkGauge("host" -> hostString, "weight" -> "broker", broker.weight)
        statsReceiver.mkGauge("host" -> hostString, "available" -> "broker", if (broker.isAvailable) 1 else 0)
      }

      broker
    }

    retrying(new LoadBalancedBroker(brokers))
  }

  def buildService[Request <: AnyRef, Reply <: AnyRef]() =
    new service.Client[Request, Reply](build())
}
