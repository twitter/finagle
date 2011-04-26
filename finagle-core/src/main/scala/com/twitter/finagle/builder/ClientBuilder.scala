package com.twitter.finagle.builder

/**
 * Provides a class for building clients.  The main class to use is
 * [[com.twitter.finagle.builder.ClientBuilder]], as so
 *
 * {{{
 * val client = ClientBuilder()
 *   .codec(Http)
 *   .hosts("localhost:10000,localhost:10001,localhost:10003")
 *   .hostConnectionLimit(1)
 *   .connectionTimeout(1.second)        // max time to spend establishing a TCP connection.
 *   .retries(2)                         // (1) per-request retries
 *   .reportTo(new OstrichStatsReceiver) // export host-level load data to ostrich
 *   .logger(Logger.getLogger("http"))
 *   .build()
 * }}}
 *
 * The `ClientBuilder` requires the definition of `cluster`, `codec`,
 * and `hostConnectionLimit`. In Scala, these are statically type
 * checked, and in Java the lack of any of the above causes a runtime
 * error.
 *
 * The `build` method uses an implicit argument to statically
 * typecheck the builder (to ensure completeness, see above). The Java
 * compiler cannot provide such implicit, so we provide a separate
 * function in Java to accomplish this. Thus, the Java code for the
 * above is
 *
 * {{{
 * Service<HttpRequest, HttpResponse> service =
 *  ClientBuilder.safeBuild(
 *    ClientBuilder.get()
 *      .codec(new Http())
 *      .hosts("localhost:10000,localhost:10001,localhost:10003")
 *      .hostConnectionLimit(1)
 *      .connectionTimeout(1.second)
 *      .retries(2)
 *      .reportTo(new OstrichStatsReceiver())
 *      .logger(Logger.getLogger("http")))
 * }}}
 *
 * Alternatively, using the `unsafeBuild` method on `ClientBuilder`
 * verifies the builder dynamically, resulting in a runtime error
 * instead of a compiler error.
 */

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

/**
 * Factory for [[com.twitter.finagle.builder.ClientBuilder]] instances
 */
object ClientBuilder {
  type Complete[Req, Rep] =
    ClientBuilder[Req, Rep, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]
  type NoCluster[Req, Rep] =
    ClientBuilder[Req, Rep, Nothing, ClientConfig.Yes, ClientConfig.Yes]
  type NoCodec =
    ClientBuilder[_, _, ClientConfig.Yes, Nothing, ClientConfig.Yes]

  def apply() = new ClientBuilder()

  /**
   * Used for Java access.
   */
  def get() = apply()

  /**
   * Provides a typesafe `build` for Java.
   */
  def safeBuild[Req, Rep](builder: Complete[Req, Rep]): Service[Req, Rep] =
    builder.build()

  /**
   * Provides a typesafe `buildFactory` for Java.
   */
  def safeBuildFactory[Req, Rep](builder: Complete[Req, Rep]): ServiceFactory[Req, Rep] =
    builder.buildFactory()

  private[finagle] val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool())))
}

object ClientConfig {
  sealed abstract trait Yes
}

/**
 * TODO: do we really need to specify HasCodec? -- it's implied in a
 * way by the proper Req, Rep
 */

final case class ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit](
  private val _cluster                   : Option[Cluster]               = None,
  private val _codec                     : Option[ClientCodec[Req, Rep]] = None,
  private val _connectionTimeout         : Duration                      = 10.milliseconds,
  private val _requestTimeout            : Duration                      = Duration.MaxValue,
  private val _statsReceiver             : Option[StatsReceiver]         = None,
  private val _name                      : Option[String]                = Some("client"),
  private val _hostConnectionCoresize    : Option[Int]                   = None,
  private val _hostConnectionLimit       : Option[Int]                   = None,
  private val _hostConnectionIdleTime    : Option[Duration]              = None,
  private val _hostConnectionMaxIdleTime : Option[Duration]              = None,
  private val _hostConnectionMaxLifeTime : Option[Duration]              = None,
  private val _sendBufferSize            : Option[Int]                   = None,
  private val _recvBufferSize            : Option[Int]                   = None,
  private val _retries                   : Option[Int]                   = None,
  private val _logger                    : Option[Logger]                = None,
  private val _channelFactory            : Option[ReferenceCountedChannelFactory] = None,
  private val _tls                       : Option[SSLContext]            = None,
  private val _startTls                  : Boolean                       = false)
{
  import ClientConfig._

  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  val cluster                   = _cluster
  val codec                     = _codec
  val connectionTimeout         = _connectionTimeout
  val requestTimeout            = _requestTimeout
  val statsReceiver             = _statsReceiver
  val name                      = _name
  val hostConnectionCoresize    = _hostConnectionCoresize
  val hostConnectionLimit       = _hostConnectionLimit
  val hostConnectionIdleTime    = _hostConnectionIdleTime
  val hostConnectionMaxIdleTime = _hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConnectionMaxLifeTime
  val sendBufferSize            = _sendBufferSize
  val recvBufferSize            = _recvBufferSize
  val retries                   = _retries
  val logger                    = _logger
  val channelFactory            = _channelFactory
  val tls                       = _tls
  val startTls                  = _startTls

  def toMap = Map(
    "cluster"                   -> _cluster,
    "codec"                     -> _codec,
    "connectionTimeout"         -> Some(_connectionTimeout),
    "requestTimeout"            -> Some(_requestTimeout),
    "statsReceiver"             -> _statsReceiver,
    "name"                      -> _name,
    "hostConnectionCoresize"    -> _hostConnectionCoresize,
    "hostConnectionLimit"       -> _hostConnectionLimit,
    "hostConnectionIdleTime"    -> _hostConnectionIdleTime,
    "hostConnectionMaxIdleTime" -> _hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime" -> _hostConnectionMaxLifeTime,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "retries"                   -> _retries,
    "logger"                    -> _logger,
    "channelFactory"            -> _channelFactory,
    "tls"                       -> _tls,
    "startTls"                  -> Some(_startTls)
  )

  override def toString = {
    "ClientConfig(%s)".format(
      toMap flatMap {
        case (k, Some(v)) =>
          Some("%s=%s".format(k, v))
        case _ =>
          None
      } mkString(", "))
  }

  def validated: ClientConfig[Req, Rep, Yes, Yes, Yes] = {
    cluster getOrElse { throw new IncompleteSpecification("No hosts were specified") }
    codec   getOrElse { throw new IncompleteSpecification("No codec was specified") }
    hostConnectionLimit getOrElse {
      throw new IncompleteSpecification("No host connection limit was specified")
    }

    copy()
  }
}

class ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit] private[builder](
  config: ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]
) {
  import ClientConfig._

  // Convenient aliases.
  type FullySpecifiedConfig = ClientConfig[Req, Rep, Yes, Yes, Yes]
  type ThisConfig           = ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]
  type This                 = ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]

  private[builder] def this() = this(new ClientConfig)

  override def toString() = "ClientBuilder(%s)".format(config.toString)

  protected def copy[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    config: ClientConfig[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1]
  ): ClientBuilder[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1] = {
    new ClientBuilder(config)
  }

  protected def withConfig[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    f: ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit] =>
       ClientConfig[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1]
  ): ClientBuilder[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1] = copy(f(config))

  def hosts(
    hostnamePortCombinations: String
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] = {
    val addresses = InetSocketAddressUtil.parseHosts(hostnamePortCombinations)
    hosts(addresses)
  }

  def hosts(
    addresses: Seq[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    cluster(new SocketAddressCluster(addresses))

  def hosts(
    address: SocketAddress
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    hosts(Seq(address))

  def cluster(
    cluster: Cluster
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    withConfig(_.copy(_cluster = Some(cluster)))

  def codec[Req1, Rep1](
    codec: ClientCodec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codec = Some(codec)))

  def protocol[Req1, Rep1](
    protocol: Protocol[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, HasCodec, HasHostConnectionLimit] = {
    val codec = new ClientCodec[Req1, Rep1] {
      def pipelineFactory = protocol.codec.clientCodec.pipelineFactory

      override def prepareService(underlying: Service[Req1, Rep1]) = {
        val future = protocol.codec.clientCodec.prepareService(underlying)
        future flatMap { protocol.prepareChannel(_) }
      }
    }

    withConfig(_.copy(_codec = Some(codec)))
  }

  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codec = Some(codec.clientCodec)))

  def connectionTimeout(duration: Duration): This =
    withConfig(_.copy(_connectionTimeout = duration))

  def requestTimeout(duration: Duration): This =
    withConfig(_.copy(_requestTimeout = duration))

  def reportTo(receiver: StatsReceiver): This =
    withConfig(_.copy(_statsReceiver = Some(receiver)))

  def name(value: String): This = withConfig(_.copy(_name = Some(value)))

  def hostConnectionLimit(value: Int): ClientBuilder[Req, Rep, HasCluster, HasCodec, Yes] =
    withConfig(_.copy(_hostConnectionLimit = Some(value)))

  def hostConnectionCoresize(value: Int): This =
    withConfig(_.copy(_hostConnectionCoresize = Some(value)))

  def hostConnectionIdleTime(timeout: Duration): This =
    withConfig(_.copy(_hostConnectionIdleTime = Some(timeout)))

  def hostConnectionMaxIdleTime(timeout: Duration): This =
    withConfig(_.copy(_hostConnectionMaxIdleTime = Some(timeout)))

  def hostConnectionMaxLifeTime(timeout: Duration): This =
    withConfig(_.copy(_hostConnectionMaxLifeTime = Some(timeout)))

  def retries(value: Int): This =
    withConfig(_.copy(_retries = Some(value)))

  def sendBufferSize(value: Int): This = withConfig(_.copy(_sendBufferSize = Some(value)))
  def recvBufferSize(value: Int): This = withConfig(_.copy(_recvBufferSize = Some(value)))

  /**
   * Use the given channel factory instead of the default. Note that
   * when using a non-default ChannelFactory, finagle can't
   * meaningfully reference count factory usage, and so the caller is
   * responsible for calling ``releaseExternalResources()''.
   */
  def channelFactory(cf: ReferenceCountedChannelFactory): This =
    withConfig(_.copy(_channelFactory = Some(cf)))

  def tls(): This =
    withConfig(_.copy(_tls = Some(Ssl.client())))

  def tlsWithoutValidation(): This =
    withConfig(_.copy(_tls = Some(Ssl.clientWithoutCertificateValidation())))

  def startTls(value: Boolean): This =
    withConfig(_.copy(_startTls = true))

  def logger(logger: Logger): This = withConfig(_.copy(_logger = Some(logger)))

  /* BUILDING */
  /* ======== */

  private[this] def buildBootstrap(codec: ClientCodec[Req, Rep], host: SocketAddress) = {
    val cf = config.channelFactory getOrElse ClientBuilder.defaultChannelFactory
    cf.acquire()
    val bs = new ClientBootstrap(cf)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.pipelineFactory.getPipeline
        for (ctx <- config.tls) {
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(true)
          // sslEngine.setEnableSessionCreation(true) // XXX - need this?
          pipeline.addFirst("ssl", new SslHandler(sslEngine, config.startTls))
        }

        for (logger <- config.logger) {
          pipeline.addFirst("channelSnooper",
            ChannelSnooper(config.name.get)(logger.info))
        }

        pipeline
      }
    }

    bs.setPipelineFactory(pf)
    bs.setOption("remoteAddress", host)
    bs.setOption("connectTimeoutMillis", config.connectionTimeout.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    bs.setOption("reuseAddress", true)
    config.sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    config.recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private[this] def buildPool(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver) = {
    // These are conservative defaults, but probably the only safe
    // thing to do.
    val lowWatermark  = config.hostConnectionCoresize getOrElse(1)
    val highWatermark = config.hostConnectionLimit    getOrElse(Int.MaxValue)
    val idleTime      = config.hostConnectionIdleTime getOrElse(5.seconds)

    val cachingPool = new CachingPool(factory, idleTime)
    new WatermarkPool[Req, Rep](cachingPool, lowWatermark, highWatermark, statsReceiver)
  }

  private[this] def prepareService(service: Service[Req, Rep]) = {
    val codec = config.codec.get
    var future: Future[Service[Req, Rep]] = null

    future = codec.prepareService(service)

    if (config.hostConnectionMaxIdleTime.isDefined ||
        config.hostConnectionMaxLifeTime.isDefined) {
      future = future map { underlying =>
        new ExpiringService( 
          underlying,
          config.hostConnectionMaxIdleTime,
          config.hostConnectionMaxLifeTime)
      }
    }

    future
  }

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols
   * (e.g., those that support transactions or authentication). 
   */
  def buildFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig => FullySpecifiedConfig
  ): ServiceFactory[Req, Rep] = {
    Timer.default.acquire()

    val cluster = config.cluster.get
    val codec   = config.codec.get

    val hostFactories = cluster mkFactories { host =>
      // The per-host stack is as follows:
      //
      //   ChannelService
      //   Pool
      //   Timeout
      //   Stats
      //
      // the pool & below are host-specific,

      val hostStatsReceiver = config.statsReceiver map { statsReceiver =>
        val hostname = host match {
          case iaddr: InetSocketAddress => "%s:%d".format(iaddr.getHostName, iaddr.getPort)
          case other => other.toString
        }

        val scoped = config.name map (statsReceiver.scope(_)) getOrElse statsReceiver
        new RollupStatsReceiver(scoped).withSuffix(hostname)
      } getOrElse NullStatsReceiver

      var factory: ServiceFactory[Req, Rep] = null

      val bs = buildBootstrap(codec, host)
      factory = new ChannelServiceFactory[Req, Rep](
        bs, prepareService _, hostStatsReceiver)
      factory = buildPool(factory, hostStatsReceiver)

      if (config.requestTimeout < Duration.MaxValue) {
        val filter = new TimeoutFilter[Req, Rep](config.requestTimeout)
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
  def build()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig => FullySpecifiedConfig
  ): Service[Req, Rep] = {
    var service: Service[Req, Rep] = new FactoryToService[Req, Rep](buildFactory())

    // We keep the retrying filter at the very bottom: this allows us
    // to retry across multiple hosts, etc.
    config.retries map { numRetries =>
      val filter = new RetryingFilter[Req, Rep](new NumTriesRetryStrategy(numRetries))
      service = filter andThen service
    }

    service
  }

  /**
   * Construct a Service, with runtime checks for builder
   * completeness.
   */
  def unsafeBuild(): Service[Req, Rep] =
    withConfig(_.validated).build()

  /**
   * Construct a ServiceFactory, with runtime checks for builder
   * completeness.
   */
  def unsafeBuildFactory(): ServiceFactory[Req, Rep] =
    withConfig(_.validated).buildFactory()
}
