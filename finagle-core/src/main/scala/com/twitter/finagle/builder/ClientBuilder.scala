package com.twitter.finagle.builder

import java.net.{InetSocketAddress, SocketAddress}
import java.util.logging.Logger
import java.util.concurrent.Executors
import javax.net.ssl.SSLContext

import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.bootstrap.ClientBootstrap

import com.twitter.util.{Future, Duration}
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.pool._
import com.twitter.finagle._
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.loadbalancer.{LoadBalancedFactory, LeastQueuedStrategy}

object ClientBuilder {
  def apply() = new ClientBuilder[Any, Any]
  def get() = apply()

  val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool())))
}

/**
 * A handy way to construct an RPC Client.
 *
 * ''Note'': A word about the default values:
 *
 *   o connectionTimeout: optimized for within a datanceter
 *   o by default, no request timeout
 */
case class ClientBuilder[Req, Rep](
  private val _cluster: Option[Cluster],
  private val _protocol: Option[Protocol[Req, Rep]],
  private val _connectionTimeout: Duration,
  private val _requestTimeout: Duration,
  private val _statsReceiver: Option[StatsReceiver],
  private val _loadStatistics: (Int, Duration),
  private val _name: Option[String],
  private val _hostConnectionCoresize: Option[Int],
  private val _hostConnectionLimit: Option[Int],
  private val _hostConnectionIdleTime: Option[Duration],
  private val _hostConnectionMaxIdleTime: Option[Duration],
  private val _hostConnectionMaxLifeTime: Option[Duration],
  private val _sendBufferSize: Option[Int],
  private val _recvBufferSize: Option[Int],
  private val _retries: Option[Int],
  private val _logger: Option[Logger],
  private val _channelFactory: Option[ReferenceCountedChannelFactory],
  private val _tls: Option[SSLContext],
  private val _startTls: Boolean)
{
  def this() = this(
    None,               // cluster
    None,               // protocol
    10.milliseconds,    // connectionTimeout
    Duration.MaxValue,  // requestTimeout
    None,               // statsReceiver
    (60, 10.seconds),   // loadStatistics
    Some("client"),     // name
    None,               // hostConnectionCoresize
    None,               // hostConnectionLimit
    None,               // hostConnectionIdleTime
    None,               // hostConnectionMaxIdleTime
    None,               // hostConnectionMaxLifeTime
    None,               // sendBufferSize
    None,               // recvBufferSize
    None,               // retries
    None,               // logger
    None,               // channelFactory
    None,               // tls
    false               // startTls
  )

  private[this] def options = Seq(
    "name"                      -> _name,
    "cluster"                   -> _cluster,
    "protocol"                  -> _protocol,
    "connectionTimeout"         -> Some(_connectionTimeout),
    "requestTimeout"            -> Some(_requestTimeout),
    "statsReceiver"             -> _statsReceiver,
    "loadStatistics"            -> _loadStatistics,
    "hostConnectionLimit"       -> Some(_hostConnectionLimit),
    "hostConnectionCoresize"    -> Some(_hostConnectionCoresize),
    "hostConnectionIdleTime"    -> Some(_hostConnectionIdleTime),
    "hostConnectionMaxIdleTime" -> Some(_hostConnectionMaxIdleTime),
    "hostConnectionMaxLifeTime" -> Some(_hostConnectionMaxLifeTime),
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "retries"                   -> _retries,
    "logger"                    -> _logger,
    "channelFactory"            -> _channelFactory,
    "tls"                       -> _tls,
    "startTls"                  -> _startTls
  )

  override def toString() = {
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

  def hosts(address: SocketAddress): ClientBuilder[Req, Rep] = hosts(Seq(address))

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

  def hostConnectionMaxIdleTime(timeout: Duration) =
    copy(_hostConnectionMaxIdleTime = Some(timeout))

  def hostConnectionMaxLifeTime(timeout: Duration) =
    copy(_hostConnectionMaxLifeTime = Some(timeout))

  def retries(value: Int) =
    copy(_retries = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  /**
   * Use the given channel factory instead of the default. Note that
   * when using a non-default ChannelFactory, finagle can't
   * meaningfully reference count factory usage, and so the caller is
   * responsible for calling ``releaseExternalResources()''.
   */
  def channelFactory(cf: ReferenceCountedChannelFactory) =
    copy(_channelFactory = Some(cf))

  def tls() =
    copy(_tls = Some(Ssl.client()))

  def tlsWithoutValidation() =
    copy(_tls = Some(Ssl.clientWithoutCertificateValidation()))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def logger(logger: Logger) = copy(_logger = Some(logger))

  // ** BUILDING
  private[this] def buildBootstrap(protocol: Protocol[Req, Rep], host: SocketAddress) = {
    val cf = _channelFactory getOrElse ClientBuilder.defaultChannelFactory
    cf.acquire()
    val bs = new ClientBootstrap(cf)
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

  private[this] def buildPool(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver) = {
    // These are conservative defaults, but probably the only safe
    // thing to do.
    val lowWatermark  = _hostConnectionCoresize getOrElse(1)
    val highWatermark = _hostConnectionLimit    getOrElse(Int.MaxValue)
    val idleTime      = _hostConnectionIdleTime getOrElse(5.seconds)

    val cachingPool = new CachingPool(factory, idleTime)
    new WatermarkPool[Req, Rep](cachingPool, lowWatermark, highWatermark, statsReceiver)
  }

  private[this] def prepareChannel(service: Service[Req, Rep]) = {
    val protocol = _protocol.get
    var future: Future[Service[Req, Rep]] = null

    future = protocol.codec.prepareClientChannel(service)
    future = future.flatMap { protocol.prepareChannel(_) }

    if (_hostConnectionMaxIdleTime.isDefined || _hostConnectionMaxLifeTime.isDefined) {
      future = future.map { new ExpiringService(_, _hostConnectionMaxIdleTime, _hostConnectionMaxLifeTime) }
    }

    future
  }

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols (e.g.,
   * those that support transactions or authentication).
   */
  def buildFactory(): ServiceFactory[Req, Rep] = {
    if (!_cluster.isDefined)
      throw new IncompleteSpecification("No hosts were specified")
    if (!_protocol.isDefined)
      throw new IncompleteSpecification("No protocol was specified")

    Timer.default.acquire()

    val cluster  = _cluster.get
    val protocol = _protocol.get

    val hostFactories = cluster mkFactories { host =>
      // The per-host stack is as follows:
      //
      //   ChannelService
      //   Pool
      //   Timeout
      //   Stats
      //
      // the pool & below are host-specific,

      val hostStatsReceiver = _statsReceiver map { statsReceiver =>
        val hostname = host match {
          case iaddr: InetSocketAddress => "%s:%d".format(iaddr.getHostName, iaddr.getPort)
          case other => other.toString
        }

        val scoped = _name map (statsReceiver.scope(_)) getOrElse statsReceiver
        new RollupStatsReceiver(scoped).withSuffix(hostname)
      } getOrElse NullStatsReceiver

      var factory: ServiceFactory[Req, Rep] = null

      val bs = buildBootstrap(protocol, host)
      factory = new ChannelServiceFactory[Req, Rep](bs, prepareChannel _, hostStatsReceiver)

      factory = buildPool(factory, hostStatsReceiver)

      if (_requestTimeout < Duration.MaxValue) {
        val filter = new TimeoutFilter[Req, Rep](_requestTimeout)
        factory = filter andThen factory
      }

      val statsFilter = new StatsFilter[Req, Rep](hostStatsReceiver)
      factory = statsFilter andThen factory

      factory
    }

    new LoadBalancedFactory(hostFactories, new LeastQueuedStrategy[Req, Rep]) {
      override def close() = {
        super.close()
        Timer.default.stop()
      }
    }
  }

  /**
   * Construct a Service.
   */
  def build(): Service[Req, Rep] = {
    var service: Service[Req, Rep] = new FactoryToService[Req, Rep](buildFactory())

    // We keep the retrying filter at the very bottom: this allows us
    // to retry across multiple hosts, etc.
    _retries map { numRetries =>
      val filter = new RetryingFilter[Req, Rep](new NumTriesRetryStrategy(numRetries))
      service = filter andThen service
    }

    service
  }
}
