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
import java.util.concurrent.{Executors, TimeUnit}
import javax.net.ssl.SSLContext

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.IdleStateHandler

import com.twitter.util.{Future, Duration, Throw, Return}
import com.twitter.util.TimeConversions._

import com.twitter.finagle.channel._
import com.twitter.finagle.util._
import com.twitter.finagle.pool._
import com.twitter.finagle._
import com.twitter.finagle.service._
import com.twitter.finagle.factory._
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.loadbalancer.{LoadBalancedFactory, LeastQueuedStrategy}
import tracing.{NullTraceReceiver, TracingFilter, TraceReceiver}

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
  type FullySpecified[Req, Rep] = ClientConfig[Req, Rep, Yes, Yes, Yes]
}

// Necessary because of the 22 argument limit on case classes
final case class ClientHostConfig(
  private val _hostConnectionCoresize    : Option[Int]                   = None,
  private val _hostConnectionLimit       : Option[Int]                   = None,
  private val _hostConnectionIdleTime    : Option[Duration]              = None,
  private val _hostConnectionMaxWaiters  : Option[Int]                   = None,
  private val _hostConnectionMaxIdleTime : Option[Duration]              = None,
  private val _hostConnectionMaxLifeTime : Option[Duration]              = None) {

  val hostConnectionCoresize    = _hostConnectionCoresize
  val hostConnectionLimit       = _hostConnectionLimit
  val hostConnectionIdleTime    = _hostConnectionIdleTime
  val hostConnectionMaxWaiters  = _hostConnectionMaxWaiters
  val hostConnectionMaxIdleTime = _hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConnectionMaxLifeTime
}

/**
 * TODO: do we really need to specify HasCodec? -- it's implied in a
 * way by the proper Req, Rep
 */
final case class ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit](
  private val _cluster                   : Option[Cluster]               = None,
  private val _codecFactory              : Option[CodecFactory[Req, Rep]#Client] = None,
  private val _connectionTimeout         : Duration                      = 10.milliseconds,
  private val _requestTimeout            : Duration                      = Duration.MaxValue,
  private val _keepAlive                 : Option[Boolean]               = None,
  private val _readerIdleTimeout         : Option[Duration]              = None,
  private val _writerIdleTimeout         : Option[Duration]              = None,
  private val _statsReceiver             : Option[StatsReceiver]         = None,
  private val _name                      : Option[String]                = Some("client"),
  private val _sendBufferSize            : Option[Int]                   = None,
  private val _recvBufferSize            : Option[Int]                   = None,
  private val _retries                   : Option[Int]                   = None,
  private val _logger                    : Option[Logger]                = None,
  private val _channelFactory            : Option[ReferenceCountedChannelFactory] = None,
  private val _tls                       : Option[SSLContext]            = None,
  private val _startTls                  : Boolean                       = false,
  private val _failureAccrualParams      : Option[(Int, Duration)]       = Some(5, 5.seconds),
  private val _traceReceiver             : TraceReceiver                 = new NullTraceReceiver,
  private val _hostConfig                : ClientHostConfig              = new ClientHostConfig)
{
  import ClientConfig._

  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  val cluster                   = _cluster
  val codecFactory              = _codecFactory
  val connectionTimeout         = _connectionTimeout
  val requestTimeout            = _requestTimeout
  val statsReceiver             = _statsReceiver
  val keepAlive                 = _keepAlive
  val readerIdleTimeout         = _readerIdleTimeout
  val writerIdleTimeout         = _writerIdleTimeout
  val name                      = _name
  val hostConnectionCoresize    = _hostConfig.hostConnectionCoresize
  val hostConnectionLimit       = _hostConfig.hostConnectionLimit
  val hostConnectionIdleTime    = _hostConfig.hostConnectionIdleTime
  val hostConnectionMaxWaiters  = _hostConfig.hostConnectionMaxWaiters
  val hostConnectionMaxIdleTime = _hostConfig.hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConfig.hostConnectionMaxLifeTime
  val hostConfig                = _hostConfig
  val sendBufferSize            = _sendBufferSize
  val recvBufferSize            = _recvBufferSize
  val retries                   = _retries
  val logger                    = _logger
  val channelFactory            = _channelFactory
  val tls                       = _tls
  val startTls                  = _startTls
  val failureAccrualParams      = _failureAccrualParams
  val traceReceiver             = _traceReceiver

  def toMap = Map(
    "cluster"                   -> _cluster,
    "codecFactory"              -> _codecFactory,
    "connectionTimeout"         -> Some(_connectionTimeout),
    "requestTimeout"            -> Some(_requestTimeout),
    "keepAlive"                 -> Some(_keepAlive),
    "readerIdleTimeout"         -> Some(_readerIdleTimeout),
    "writerIdleTimeout"         -> Some(_writerIdleTimeout),
    "statsReceiver"             -> _statsReceiver,
    "name"                      -> _name,
    "hostConnectionCoresize"    -> _hostConfig.hostConnectionCoresize,
    "hostConnectionLimit"       -> _hostConfig.hostConnectionLimit,
    "hostConnectionIdleTime"    -> _hostConfig.hostConnectionIdleTime,
    "hostConnectionMaxWaiters"  -> _hostConfig.hostConnectionMaxWaiters,
    "hostConnectionMaxIdleTime" -> _hostConfig.hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime" -> _hostConfig.hostConnectionMaxLifeTime,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "retries"                   -> _retries,
    "logger"                    -> _logger,
    "channelFactory"            -> _channelFactory,
    "tls"                       -> _tls,
    "startTls"                  -> Some(_startTls),
    "failureAccrualParams"      -> _failureAccrualParams,
    "traceReceiver"             -> Some(traceReceiver)
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
    cluster      getOrElse { throw new IncompleteSpecification("No hosts were specified") }
    codecFactory getOrElse { throw new IncompleteSpecification("No codec was specified") }
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
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
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
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(Function.const(codec) _)))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(codecFactory.client)))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Client
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(codecFactory)))

  def connectionTimeout(duration: Duration): This =
    withConfig(_.copy(_connectionTimeout = duration))

  def requestTimeout(duration: Duration): This =
    withConfig(_.copy(_requestTimeout = duration))

  def keepAlive(value: Boolean): This =
    withConfig(_.copy(_keepAlive = Some(value)))

  def readerIdleTimeout(duration: Duration): This =
    withConfig(_.copy(_readerIdleTimeout = Some(duration)))

  def writerIdleTimeout(duration: Duration): This =
    withConfig(_.copy(_writerIdleTimeout = Some(duration)))

  def reportTo(receiver: StatsReceiver): This =
    withConfig(_.copy(_statsReceiver = Some(receiver)))

  def name(value: String): This = withConfig(_.copy(_name = Some(value)))

  def hostConnectionLimit(value: Int): ClientBuilder[Req, Rep, HasCluster, HasCodec, Yes] =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionLimit = Some(value))))

  def hostConnectionCoresize(value: Int): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionCoresize = Some(value))))

  def hostConnectionIdleTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionIdleTime = Some(timeout))))

  def hostConnectionMaxWaiters(nWaiters: Int): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxWaiters = Some(nWaiters))))

  def hostConnectionMaxIdleTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxIdleTime = Some(timeout))))

  def hostConnectionMaxLifeTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxLifeTime = Some(timeout))))

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

  def traceReceiver(receiver: TraceReceiver): This =
    withConfig(_.copy(_traceReceiver = receiver))

  def logger(logger: Logger): This = withConfig(_.copy(_logger = Some(logger)))

  def failureAccrualParams(params: (Int, Duration)): This =
    withConfig(_.copy(_failureAccrualParams = Some(params)))

  /* BUILDING */
  /* ======== */

  private[this] def buildBootstrap(codec: Codec[Req, Rep], host: SocketAddress) = {
    val cf = config.channelFactory getOrElse ClientBuilder.defaultChannelFactory
    cf.acquire()
    val bs = new ClientBootstrap(cf)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.pipelineFactory.getPipeline

        if (config.readerIdleTimeout.isDefined || config.writerIdleTimeout.isDefined) {
          pipeline.addFirst("idleReactor", new IdleChannelHandler)
          pipeline.addFirst("idleDetector",
            new IdleStateHandler(Timer.defaultNettyTimer,
              config.readerIdleTimeout.map(_.inMilliseconds).getOrElse(0L),
              config.writerIdleTimeout.map(_.inMilliseconds).getOrElse(0L),
              0,
              TimeUnit.MILLISECONDS))
        }

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
    config.keepAlive foreach { value => bs.setOption("keepAlive", value) }
    bs.setOption("reuseAddress", true)
    config.sendBufferSize foreach { s => bs.setOption("sendBufferSize", s)    }
    config.recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private[this] def buildPool(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver) = {
    // These are conservative defaults, but probably the only safe
    // thing to do.
    val lowWatermark  = config.hostConnectionCoresize   getOrElse(1)
    val highWatermark = config.hostConnectionLimit      getOrElse(Int.MaxValue)
    val idleTime      = config.hostConnectionIdleTime   getOrElse(5.seconds)
    val maxWaiters    = config.hostConnectionMaxWaiters getOrElse(Int.MaxValue)

    val cachingPool = new CachingPool(factory, idleTime)
    new WatermarkPool[Req, Rep](
      cachingPool, lowWatermark, highWatermark,
      statsReceiver, maxWaiters)
  }

  private[this] def prepareService(codec: Codec[Req, Rep])(service: Service[Req, Rep]) = {
    var future: Future[Service[Req, Rep]] = codec.prepareService(service)

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

  private[this] lazy val statsReceiver = {
    val statsReceiver = config.statsReceiver getOrElse NullStatsReceiver
    config.name match {
      case Some(name) => statsReceiver.scope(name)
      case None       => statsReceiver
    }
  }

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols
   * (e.g., those that support transactions or authentication).
   */
  def buildFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): ServiceFactory[Req, Rep] = {
    Timer.default.acquire()

    val cluster = config.cluster.get
    val codec   = config.codecFactory.get(ClientCodecConfig(serviceName = config.name))

    val hostFactories = cluster mkFactories { host =>
      // The per-host stack is as follows:
      //
      //   ChannelService
      //   Pool
      //   Timeout
      //   Failure accrual
      //   Stats
      //
      // the pool & below are host-specific,

      val hostStatsReceiver = new RollupStatsReceiver(statsReceiver).withSuffix(
        host match {
          case iaddr: InetSocketAddress => "%s:%d".format(iaddr.getHostName, iaddr.getPort)
          case other => other.toString
        }
      )

      var factory: ServiceFactory[Req, Rep] = null

      val bs = buildBootstrap(codec, host)
      factory = new ChannelServiceFactory[Req, Rep](
        bs, prepareService(codec) _, hostStatsReceiver)
      factory = buildPool(factory, hostStatsReceiver)

      if (config.requestTimeout < Duration.MaxValue) {
        val filter = new TimeoutFilter[Req, Rep](config.requestTimeout)
        factory = filter andThen factory
      }

      config.failureAccrualParams foreach { case (numFailures, markDeadFor) =>
        factory = new FailureAccrualFactory(factory, numFailures, markDeadFor)
      }

      val statsFilter = new StatsFilter[Req, Rep](hostStatsReceiver)
      factory = statsFilter andThen factory

      factory
    }

    val loadbalanced = new LoadBalancedFactory(
      hostFactories,
      statsReceiver.scope("loadbalancer"),
      new LeastQueuedStrategy(statsReceiver.scope("least_queued_strategy")))
    {
      override def close() = {
        super.close()
        Timer.default.stop()
      }
    }

    // We maintain a separate log of factory failures here so that
    // factory failures are captured in the service failure
    // stats. Logically these are service failures, but since the
    // requests are never dispatched to the underlying stack, they
    // don't get recorded there.
    new StatsFactoryWrapper(loadbalanced, statsReceiver)
  }

  /**
   * Construct a Service.
   */
  def build()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Service[Req, Rep] = {
    var service: Service[Req, Rep] = new FactoryToService[Req, Rep](buildFactory())

    // We keep the retrying filter at the very bottom: this allows us
    // to retry across multiple hosts, etc.
    config.retries map { numRetries =>
      val filter = RetryingService.tries[Req, Rep](
        numRetries,
        statsReceiver)
      service = filter andThen service
    }

    (new TracingFilter(config.traceReceiver)) andThen service
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
