package com.twitter.finagle.builder

import java.net.SocketAddress
import java.util.logging.Logger
import java.util.concurrent.Executors
import javax.net.ssl.SSLContext

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.bootstrap.ClientBootstrap

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.pool._
import com.twitter.finagle.{Service, ServiceFactory, Codec, Protocol}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.loadbalancer.{LoadBalancedFactory, LeastQueuedStrategy}

object ClientBuilder {
  def apply() = new ClientBuilder[Any, Any]
  def get() = apply()

  lazy val defaultChannelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())
}

/**
 * A word about the default values:
 *
 *   o connectionTimeout: optimized for within a datanceter
 *   o by default, no request timeout
 */
case class ClientBuilder[Req, Rep](
  _cluster: Option[Cluster],
  _protocol: Option[Protocol[Req, Rep]],
  _connectionTimeout: Duration,
  _requestTimeout: Duration,
  _statsReceiver: Option[StatsReceiver],
  _loadStatistics: (Int, Duration),
  _name: Option[String],
  _hostConnectionCoresize: Option[Int],
  _hostConnectionLimit: Option[Int],
  _hostConnectionIdleTime: Option[Duration],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _retries: Option[Int],
  _logger: Option[Logger],
  _channelFactory: Option[ChannelFactory],
  _tls: Option[SSLContext],
  _startTls: Boolean)
{
  def this() = this(
    None,                                        // cluster
    None,                                        // protocol
    10.milliseconds,                             // connectionTimeout
    Duration.MaxValue,                           // requestTimeout
    None,                                        // statsReceiver
    (60, 10.seconds),                            // loadStatistics
    Some("client"),                              // name
    None,                                        // hostConnectionCoresize
    None,                                        // hostConnectionLimit
    None,                                        // hostConnectionIdleTime
    None,                                        // sendBufferSize
    None,                                        // recvBufferSize
    None,                                        // retries
    None,                                        // logger
    Some(ClientBuilder.defaultChannelFactory),   // channelFactory
    None,                                        // tls
    false                                        // startTls
  )

  override def toString() = {
    val options = Seq(
      "name"                   -> _name,
      "cluster"                -> _cluster,
      "protocol"               -> _protocol,
      "connectionTimeout"      -> Some(_connectionTimeout),
      "requestTimeout"         -> Some(_requestTimeout),
      "statsReceiver"          -> _statsReceiver,
      "loadStatistics"         -> _loadStatistics,
      "hostConnectionLimit"    -> Some(_hostConnectionLimit),
      "hostConnectionCoresize" -> Some(_hostConnectionCoresize),
      "hostConnectionIdleTime" -> Some(_hostConnectionIdleTime),
      "sendBufferSize"         -> _sendBufferSize,
      "recvBufferSize"         -> _recvBufferSize,
      "retries"                -> _retries,
      "logger"                 -> _logger,
      "channelFactory"         -> _channelFactory,
      "tls"                    -> _tls,
      "startTls"               -> _startTls
    )

    "ClientBuilder(%s)".format(
      options flatMap {
        case (k, Some(v)) => Some("%s=%s".format(k, v))
        case _ => None
      } mkString(", "))
  }

  def hosts(hostnamePortCombinations: String): ClientBuilder[Req, Rep] = {
    val addresses = InetSocketAddressUtil.parseHosts(
      hostnamePortCombinations)
    hosts(addresses)
  }

  def hosts(addresses: Seq[SocketAddress]): ClientBuilder[Req, Rep] = {
    val _cluster = new SocketAddressCluster(addresses)
    cluster(_cluster)
  }

  def cluster(cluster: Cluster): ClientBuilder[Req, Rep] = {
    copy(_cluster = Some(cluster))
  }

  def protocol[Req1, Rep1](protocol: Protocol[Req1, Rep1]) =
    copy(_protocol = Some(protocol))

  def codec[Req1, Rep1](_codec: Codec[Req1, Rep1]) =
    copy(_protocol = Some(new Protocol[Req1, Rep1] {
      def codec = _codec
    }))

  def connectionTimeout(duration: Duration) =
    copy(_connectionTimeout = duration)

  def requestTimeout(duration: Duration) =
    copy(_requestTimeout = duration)

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  /**
   * The interval over which to aggregate load statistics.
   */
  def loadStatistics(numIntervals: Int, interval: Duration) = {
    require(numIntervals >= 1, "Must have at least 1 window to sample statistics over")

    copy(_loadStatistics = (numIntervals, interval))
  }

  def name(value: String) = copy(_name = Some(value))

  def hostConnectionLimit(value: Int) =
    copy(_hostConnectionLimit = Some(value))

  def hostConnectionCoresize(value: Int) =
    copy(_hostConnectionCoresize = Some(value))

  def hostConnectionIdleTime(timeout: Duration) =
    copy(_hostConnectionIdleTime = Some(timeout))

  def retries(value: Int) =
    copy(_retries = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def channelFactory(cf: ChannelFactory) =
    copy(_channelFactory = Some(cf))

  def tls() =
    copy(_tls = Some(Ssl.client()))

  def tlsWithoutValidation() =
    copy(_tls = Some(Ssl.clientWithoutCertificateValidation()))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def logger(logger: Logger) = copy(_logger = Some(logger))

  // ** BUILDING
  private[this] def bootstrap(protocol: Protocol[Req, Rep])(host: SocketAddress) = {
    val bs = new ClientBootstrap(_channelFactory.get)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = protocol.codec.clientPipelineFactory.getPipeline
        for (ctx <- _tls) {
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(true)
          // sslEngine.setEnableSessionCreation(true) // XXX - need this?
          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        for (logger <- _logger) {
          pipeline.addFirst("channelSnooper",
            ChannelSnooper(_name.get)(logger.info))
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

  // private[this] def prepareChannel(channelService: ChannelService[Req, Rep]) = {
  //   if (_requestTimeout < Duration.MaxValue) {
  //     val timeoutFilter = new TimeoutFilter[Req, Rep](Timer.default, _requestTimeout)
  //     broker = timeoutFilter andThen broker
  //   }

  //   if (_statsReceiver.isDefined) {
  //     val statsFilter = new StatsFilter[Req, Rep](
  //       _statsReceiver.get.scope("host" -> host.toString))
  //     broker = statsFilter andThen broker
  //   }

  //   _protocol.get.prepareChannel(_)
  // }

  private[this] def pool(bootstrap: ClientBootstrap) = {
    // These are conservative defaults, but probably the only safe
    // thing to do.
    val lowWatermark  = _hostConnectionCoresize getOrElse(1)
    val highWatermark = _hostConnectionLimit    getOrElse(Int.MaxValue)
    val idleTime      = _hostConnectionIdleTime getOrElse(5.seconds)

    val channelServiceFactory =
      new ChannelServiceFactory[Req, Rep](
        bootstrap, _protocol.get.prepareChannel(_))

    val cachingPool = new CachingPool(channelServiceFactory, idleTime)
    new WatermarkPool[Req, Rep](cachingPool, lowWatermark, highWatermark)
  }

  private[this] def retryingFilter = _retries map { numRetries =>
    new RetryingFilter[Req, Rep](new NumTriesRetryStrategy(numRetries))
  }

  def build(): ServiceFactory[Req, Rep] = {
    if (!_cluster.isDefined)
      throw new IncompleteSpecification("No hosts were specified")
    if (!_protocol.isDefined)
      throw new IncompleteSpecification("No protocol was specified")

    val cluster  = _cluster.get
    val protocol = _protocol.get

          // // XXX - retries
          // val filtered =
          //   if (_requestTimeout < Duration.MaxValue) {
          //     val timeoutFilter = new TimeoutFilter[Req, Rep](Timer.default, _requestTimeout)
          //     timeoutFilter andThen service
          //   } else {
          //     service
          //   }

    val pools = cluster mapHosts { host => pool(bootstrap(protocol)(host)) }
    val loadBalanced = new LoadBalancedFactory(pools, new LeastQueuedStrategy[Req, Rep])

    new ServiceFactory[Req, Rep] {
      override def apply(request: Req) =
        // Apply retries when needed.
        make() flatMap { service =>
          val retrying = retryingFilter map { _ andThen service } getOrElse { service }
          retrying(request) ensure { service.release() }
        }

      def make() = loadBalanced.make()
      override def release() = loadBalanced.release()
      override def isAvailable = loadBalanced.isAvailable
    }
  }
}
