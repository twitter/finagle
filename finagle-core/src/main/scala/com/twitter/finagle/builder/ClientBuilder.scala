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
 *   .tcpConnectTimeout(1.second)        // max time to spend establishing a TCP connection.
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
 *      .tcpConnectTimeout(1.second)
 *      .retries(2)
 *      .reportTo(new OstrichStatsReceiver())
 *      .logger(Logger.getLogger("http")))
 * }}}
 *
 * Alternatively, using the `unsafeBuild` method on `ClientBuilder`
 * verifies the builder dynamically, resulting in a runtime error
 * instead of a compiler error.
 */

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle._
import com.twitter.finagle.channel._
import com.twitter.finagle.factory._
import com.twitter.finagle.filter.{MonitorFilter, ExceptionSourceFilter}
import com.twitter.finagle.loadbalancer.HeapBalancer
import com.twitter.finagle.pool._
import com.twitter.finagle.service._
import com.twitter.finagle.ssl.{Engine, Ssl, SslConnectHandler}
import com.twitter.finagle.stats.{
  GlobalStatsReceiver, NullStatsReceiver, RollupStatsReceiver, StatsReceiver}
import com.twitter.finagle.util._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Duration, Future, Monitor, Time, Timer, Try}
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.logging.Logger
import javax.net.ssl.SSLContext
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.{util => nu}
import scala.annotation.implicitNotFound
import tracing.{NullTracer, TracingFilter, Tracer}

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
    builder.build()(ClientConfigEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildFactory` for Java.
   */
  def safeBuildFactory[Req, Rep](builder: Complete[Req, Rep]): ServiceFactory[Req, Rep] =
    builder.buildFactory()(ClientConfigEvidence.FullyConfigured)

  private[finagle] val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleClientBoss")),
          Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleClientIO"))
        )
      )
    )
}

object ClientConfig {
  sealed abstract trait Yes
  type FullySpecified[Req, Rep] = ClientConfig[Req, Rep, Yes, Yes, Yes]
}

@implicitNotFound("Builder is not fully configured: Cluster: ${HasCluster}, Codec: ${HasCodec}, HostConnectionLimit: ${HasHostConnectionLimit}")
trait ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]

object ClientConfigEvidence {
  implicit object FullyConfigured extends ClientConfigEvidence[ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]
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
 * way by the proper Req, Rep.
 *
 * Note: these are documented in ClientBuilder, as that is where they
 * are accessed by the end-user.
 */
final case class ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit](
  private val _cluster                   : Option[Cluster[SocketAddress]]        = None,
  private val _codecFactory              : Option[CodecFactory[Req, Rep]#Client] = None,
  private val _tcpConnectTimeout         : Duration                      = 10.milliseconds,
  private val _connectTimeout            : Duration                      = Duration.MaxValue,
  private val _requestTimeout            : Duration                      = Duration.MaxValue,
  private val _timeout                   : Duration                      = Duration.MaxValue,
  private val _keepAlive                 : Option[Boolean]               = None,
  private val _readerIdleTimeout         : Option[Duration]              = None,
  private val _writerIdleTimeout         : Option[Duration]              = None,
  private val _statsReceiver             : Option[StatsReceiver]         = None,
  private val _monitor                   : Option[String => Monitor]     = None,
  private val _name                      : Option[String]                = Some("client"),
  private val _sendBufferSize            : Option[Int]                   = None,
  private val _recvBufferSize            : Option[Int]                   = None,
  private val _retryPolicy               : Option[RetryPolicy[Try[Nothing]]]  = None,
  private val _logger                    : Option[Logger]                = None,
  private val _channelFactory            : Option[ReferenceCountedChannelFactory] = None,
  private val _tls                       : Option[(() => Engine, Option[String])] = None,
  private val _failureAccrual            : Option[Timer => ServiceFactoryWrapper] = Some(FailureAccrualFactory.wrapper(5, 5.seconds)),
  private val _tracerFactory             : Managed[Tracer]               = Managed.const(NullTracer),
  private val _hostConfig                : ClientHostConfig              = new ClientHostConfig,
  private val _expFailFast               : Boolean                       = false)
{
  import ClientConfig._

  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  val cluster                   = _cluster
  val codecFactory              = _codecFactory
  val tcpConnectTimeout         = _tcpConnectTimeout
  val requestTimeout            = _requestTimeout
  val connectTimeout            = _connectTimeout
  val timeout                   = _timeout
  val statsReceiver             = _statsReceiver
  val monitor                   = _monitor
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
  val retryPolicy               = _retryPolicy
  val logger                    = _logger
  val channelFactory            = _channelFactory
  val tls                       = _tls
  val failureAccrual            = _failureAccrual
  val tracerFactory             = _tracerFactory
  val expFailFast               = _expFailFast

  def toMap = Map(
    "cluster"                   -> _cluster,
    "codecFactory"              -> _codecFactory,
    "tcpConnectTimeout"         -> Some(_tcpConnectTimeout),
    "requestTimeout"            -> Some(_requestTimeout),
    "connectTimeout"            -> Some(_connectTimeout),
    "timeout"                   -> Some(_timeout),
    "keepAlive"                 -> Some(_keepAlive),
    "readerIdleTimeout"         -> Some(_readerIdleTimeout),
    "writerIdleTimeout"         -> Some(_writerIdleTimeout),
    "statsReceiver"             -> _statsReceiver,
    "monitor"                   -> _monitor,
    "name"                      -> _name,
    "hostConnectionCoresize"    -> _hostConfig.hostConnectionCoresize,
    "hostConnectionLimit"       -> _hostConfig.hostConnectionLimit,
    "hostConnectionIdleTime"    -> _hostConfig.hostConnectionIdleTime,
    "hostConnectionMaxWaiters"  -> _hostConfig.hostConnectionMaxWaiters,
    "hostConnectionMaxIdleTime" -> _hostConfig.hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime" -> _hostConfig.hostConnectionMaxLifeTime,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "retryPolicy"               -> _retryPolicy,
    "logger"                    -> _logger,
    "channelFactory"            -> _channelFactory,
    "tls"                       -> _tls,
    "failureAccrual"            -> _failureAccrual,
    "tracerFactory"             -> Some(_tracerFactory),
    "expFailFast"               -> expFailFast
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

class ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit] private[finagle](
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

  /**
   * Specify the set of hosts to connect this client to.  Requests
   * will be load balanced across these.  This is a shorthand form for
   * specifying a cluster.
   *
   * One of the {{hosts}} variations or direct specification of the
   * cluster (via {{cluster}}) is required.
   *
   * @param hostNamePortcombinations comma-separated "host:port"
   * string.
   */
  def hosts(
    hostnamePortCombinations: String
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] = {
    val addresses = InetSocketAddressUtil.parseHosts(hostnamePortCombinations)
    hosts(addresses)
  }

  /**
   * A variant of {{hosts}} that takes a sequence of
   * [[java.net.SocketAddress]] instead.
   */
  def hosts(
    addresses: Seq[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    cluster(new StaticCluster[SocketAddress](addresses))

  /**
   * A convenience method for specifying a one-host
   * [[java.net.SocketAddress]] client.
   */
  def hosts(
    address: SocketAddress
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    hosts(Seq(address))

  /**
   * Specify a cluster directly.  A
   * [[com.twitter.finagle.builder.Cluster]] defines a dynamic
   * mechanism for specifying a set of endpoints to which this client
   * remains connected.
   */
  def cluster(
    cluster: Cluster[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    withConfig(_.copy(_cluster = Some(cluster)))

  /**
   * Specify the codec. The codec implements the network protocol
   * used by the client, and consequently determines the {{Req}} and {{Rep}}
   * type variables. One of the codec variations is required.
   */
  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(Function.const(codec) _)))

  /**
   * A variation of {{codec}} that supports codec factories.  This is
   * used by codecs that need dynamic construction, but should be
   * transparent to the user.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(codecFactory.client)))

  /**
   * A variation of codec for codecs that support only client-codecs.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Client
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    withConfig(_.copy(_codecFactory = Some(codecFactory)))

  @deprecated("Use tcpConnectTimeout instead", "5.0.1")
  def connectionTimeout(duration: Duration): This = tcpConnectTimeout(duration)

  /**
   * Specify the TCP connection timeout.
   */
  def tcpConnectTimeout(duration: Duration): This =
    withConfig(_.copy(_tcpConnectTimeout = duration))

  /**
   * The request timeout is the time given to a *single* request (if
   * there are retries, they each get a fresh request timeout).  The
   * timeout is applied only after a connection has been acquired.
   * That is: it is applied to the interval between the dispatch of
   * the request and the receipt of the response.
   */
  def requestTimeout(duration: Duration): This =
    withConfig(_.copy(_requestTimeout = duration))

  /**
   * The connect timeout is the timeout applied to the acquisition of
   * a Service.  This includes both queueing time (eg.  because we
   * cannot create more connections due to {{hostConnectionLimit}} and
   * there are more than {{hostConnectionLimit}} requests outstanding)
   * as well as physical connection time.  Futures returned from
   * {{factory()}} will always be satisfied within this timeout.
   */
  def connectTimeout(duration: Duration): This =
    withConfig(_.copy(_connectTimeout = duration))

  /**
   * Total request timeout.  This timeout is applied from the issuance
   * of a request (through {{service(request)}}) until the
   * satisfaction of that reply future.  No request will take longer
   * than this.
   *
   * Applicable only to service-builds ({{build()}})
   */
  def timeout(duration: Duration): This =
    withConfig(_.copy(_timeout = duration))

  /**
   * Apply TCP keepAlive ({{SO_KEEPALIVE}} socket option).
   */
  def keepAlive(value: Boolean): This =
    withConfig(_.copy(_keepAlive = Some(value)))

  /**
   * The maximum time a connection may have received no data.
   */
  def readerIdleTimeout(duration: Duration): This =
    withConfig(_.copy(_readerIdleTimeout = Some(duration)))

  /**
   * The maximum time a connection may not have sent any data.
   */
  def writerIdleTimeout(duration: Duration): This =
    withConfig(_.copy(_writerIdleTimeout = Some(duration)))

  /**
   * Report stats to the given {{StatsReceiver}}.  This will report
   * verbose client statistics and counters, that in turn may be
   * exported to monitoring applications.
   */
  def reportTo(receiver: StatsReceiver): This =
    withConfig(_.copy(_statsReceiver = Some(receiver)))

  /**
   * Give a meaningful name to the client. Required.
   */
  def name(value: String): This = withConfig(_.copy(_name = Some(value)))

  /**
   * The maximum number of connections that are allowed per host.
   * Required.  Finagle guarantees to to never have more active
   * connections than this limit.
   */
  def hostConnectionLimit(value: Int): ClientBuilder[Req, Rep, HasCluster, HasCodec, Yes] =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionLimit = Some(value))))

  /**
   * The core size of the connection pool: the pool is not shrinked below this limit.
   */
  def hostConnectionCoresize(value: Int): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionCoresize = Some(value))))

  /**
   * The amount of time a connection is allowed to linger (when it
   * otherwise would have been closed by the pool) before being
   * closed.
   */
  def hostConnectionIdleTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionIdleTime = Some(timeout))))

  /**
   * The maximum queue size for the connection pool.
   */
  def hostConnectionMaxWaiters(nWaiters: Int): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxWaiters = Some(nWaiters))))

  /**
   * The maximum time a connection is allowed to linger unused.
   */
  def hostConnectionMaxIdleTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxIdleTime = Some(timeout))))

  /**
   * The maximum time a connection is allowed to exist, regardless of occupancy.
   */
  def hostConnectionMaxLifeTime(timeout: Duration): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionMaxLifeTime = Some(timeout))))

  /**
   * The number of retries applied. Only applicable to service-builds ({{build()}})
   */
  def retries(value: Int): This =
    retryPolicy(RetryPolicy.tries(value))

  def retryPolicy(value: RetryPolicy[Try[Nothing]]): This =
    withConfig(_.copy(_retryPolicy = Some(value)))

  /**
   * Sets the TCP send buffer size.
   */
  def sendBufferSize(value: Int): This = withConfig(_.copy(_sendBufferSize = Some(value)))
  /**
   * Sets the TCP recv buffer size.
   */
  def recvBufferSize(value: Int): This = withConfig(_.copy(_recvBufferSize = Some(value)))

  /**
   * Use the given channel factory instead of the default. Note that
   * when using a non-default ChannelFactory, finagle can't
   * meaningfully reference count factory usage, and so the caller is
   * responsible for calling ``releaseExternalResources()''.
   */
  def channelFactory(cf: ReferenceCountedChannelFactory): This =
    withConfig(_.copy(_channelFactory = Some(cf)))

  /**
   * Encrypt the connection with SSL.  Hostname verification will be
   * provided against the given hostname.
   */
  def tls(hostname: String): This =
    withConfig(_.copy(_tls = Some({ () => Ssl.client()}, Some(hostname))))

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * No SSL Hostname Validation is performed
   */
  def tls(sslContext : SSLContext): This =
    withConfig(_.copy(_tls = Some({ () => Ssl.client(sslContext)  }, None)))

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * SSL Hostname Validation is performed, on the passed in hostname
   */
  def tls(sslContext : SSLContext, hostname : Option[String]): This =
    withConfig(_.copy(_tls = Some({ () => Ssl.client(sslContext)  }, hostname)))

  /**
   * Do not perform TLS validation. Probably dangerous.
   */
  def tlsWithoutValidation(): This =
    withConfig(_.copy(_tls = Some({ () => Ssl.clientWithoutCertificateValidation()}, None)))

  /**
   * Specifies a tracer that receives trace events.
   * See [[com.twitter.finagle.tracing]] for details.
   */
  def tracerFactory(factory: Tracer.Factory): This =
    withConfig(_.copy(_tracerFactory = Tracer.mkManaged(factory)))

  def monitor(mFactory: String => Monitor): This =
    withConfig(_.copy(_monitor = Some(mFactory)))

  /**
   * Log very detailed debug information to the given logger.
   */
  def logger(logger: Logger): This = withConfig(_.copy(_logger = Some(logger)))

  /**
   * Use the given paramters for failure accrual.  The first parameter
   * is the number of *successive* failures that are required to mark
   * a host failed.  The second paramter specifies how long the host
   * is dead for, once marked.
   */
  def failureAccrualParams(params: (Int, Duration)): This = {
    failureAccrualFactory(FailureAccrualFactory.wrapper(params._1, params._2) _)
  }

  def failureAccrual(failureAccrual: ServiceFactoryWrapper): This = {
    failureAccrualFactory { (_) => failureAccrual }
  }

  def failureAccrualFactory(factory: Timer => ServiceFactoryWrapper): This = {
    withConfig(_.copy(_failureAccrual = Some(factory)))
  }

  /**
   * An experimental "fail-fast" mode that marks a host dead on
   * connection failure. The host remains dead until we
   * succesfully connect.
   *
   * Intermediate connection attempts *are* respected, but host
   * availability is turned off during the reconnection period.
   */
  def expFailFast(onOrOff: Boolean): This =
   withConfig(_.copy(_expFailFast = onOrOff))

  /* BUILDING */
  /* ======== */

  private[this] def buildBootstrap(
    codec: Codec[Req, Rep],
    host: SocketAddress,
    timer: TwoTimer
  ) = {
    val cf = config.channelFactory getOrElse ClientBuilder.defaultChannelFactory
    cf.acquire()

    val bs = new ClientBootstrap(cf)
    val pf = new ChannelPipelineFactory {
      override def getPipeline = {
        val pipeline = codec.pipelineFactory.getPipeline

        pipeline.addFirst("channelStatsHandler", new ChannelStatsHandler(statsReceiver))
        pipeline.addFirst("channelRequestStatsHandler",
          new ChannelRequestStatsHandler(statsReceiver)
        )

        if (config.readerIdleTimeout.isDefined || config.writerIdleTimeout.isDefined) {
          pipeline.addFirst("idleReactor", new IdleChannelHandler)
          pipeline.addFirst("idleDetector",
            new IdleStateHandler(timer,
              config.readerIdleTimeout.map(_.inMilliseconds).getOrElse(0L),
              config.writerIdleTimeout.map(_.inMilliseconds).getOrElse(0L),
              0,
              TimeUnit.MILLISECONDS))
        }

        for ((engineFactory, hostname) <- config.tls) {
          val engine = engineFactory()
          engine.self.setUseClientMode(true)
          engine.self.setEnableSessionCreation(true)
          val sslHandler = new SslHandler(engine.self)
          val verifier = hostname map {
            SslConnectHandler.sessionHostnameVerifier(_) _
          } getOrElse { Function.const(None) _ }

          pipeline.addFirst("sslConnect",
            new SslConnectHandler(sslHandler, verifier))
          pipeline.addFirst("ssl", sslHandler)
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
    bs.setOption("connectTimeoutMillis", config.tcpConnectTimeout.inMilliseconds)
    bs.setOption("tcpNoDelay", true)  // fin NAGLE.  get it?
    // bs.setOption("soLinger", 0)  (TODO)
    config.keepAlive foreach { value => bs.setOption("keepAlive", value) }
    bs.setOption("reuseAddress", true)
    config.sendBufferSize foreach { s => bs.setOption("sendBufferSize", s)    }
    config.recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }
    bs
  }

  private[this] def buildPool(
    factory: ServiceFactory[Req, Rep], timer: TwoTimer, statsReceiver: StatsReceiver) = {
    // These are conservative defaults, but probably the only safe
    // thing to do.
    val lowWatermark = config.hostConnectionCoresize   getOrElse(1)
    val highWatermark = Seq(lowWatermark, config.hostConnectionLimit getOrElse(Int.MaxValue)).max
    val idleTime = config.hostConnectionIdleTime   getOrElse(5.seconds)
    val maxWaiters = config.hostConnectionMaxWaiters getOrElse(Int.MaxValue)

    val underlyingFactory = if (idleTime > 0.seconds && highWatermark > lowWatermark) {
      new CachingPool(
        factory,
        highWatermark - lowWatermark,
        idleTime,
        timer.toTwitterTimer,
        statsReceiver)
    } else {
      factory
    }

    new WatermarkPool[Req, Rep](
      underlyingFactory, lowWatermark, highWatermark,
      statsReceiver, maxWaiters)
  }

  private[finagle] lazy val statsReceiver = {
    val statsReceiver = config.statsReceiver getOrElse NullStatsReceiver
    config.name match {
      case Some(name) => statsReceiver.scope(name)
      case None       => statsReceiver
    }
  }

  private[this] def hostToServiceFactory(
    codec: Codec[Req, Rep],
    host: SocketAddress,
    timer: TwoTimer
  ): ServiceFactory[Req, Rep] = {
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
    val bs = buildBootstrap(codec, host, timer)
    val preparedFactory = codec.prepareConnFactory(new ChannelServiceFactory[Req, Rep](
      bs, codec.mkClientDispatcher, hostStatsReceiver
    ))
    factory = new ServiceFactoryProxy[Req, Rep](preparedFactory) {
      override def apply(con: ClientConnection) =
        hostStatsReceiver.timeFuture("codec_connection_preparation_latency_ms") {
          self.apply(con)
        }
    }

    if (config.hostConnectionMaxIdleTime.isDefined ||
        config.hostConnectionMaxLifeTime.isDefined) {
      factory = factory map { service =>
        new ExpiringService(
          new CloseOnReleaseService(service),
          config.hostConnectionMaxIdleTime,
          config.hostConnectionMaxLifeTime,
          timer.toTwitterTimer)
      }
    }

    factory = buildPool(factory, timer, hostStatsReceiver)
    factory = requestTimeoutFilter(timer) andThen factory
    factory = failureAccrualFactory(factory, timer)

    if (config.expFailFast) {
      factory = new FailFastFactory(
        factory, hostStatsReceiver.scope("failfast"), timer.toTwitterTimer)
    }

    val statsFilter = new StatsFilter[Req, Rep](hostStatsReceiver)
    factory = statsFilter andThen factory

    factory = monitorFilter andThen factory

    factory
  }

  private[this] def rawInternalBuildFactory(tracer: Tracer, timer: TwoTimer) = {
    GlobalStatsReceiver.register(statsReceiver.scope("finagle"))

    val cluster = config.cluster.get
    val codec = config.codecFactory.get(ClientCodecConfig(serviceName = config.name.get))

    val hostFactories = cluster map { host => hostToServiceFactory(codec, host, timer) }
    var factory: ServiceFactory[Req, Rep] =
      new HeapBalancer(hostFactories, statsReceiver.scope("loadbalancer"))

    /*
     * Everything above this point in the stack (load balancer, pool)
     * expect that the we only release after the last request is done.
     * Thus, the Refcounted factory serves as a rectifier.
     */
    factory = new RefcountedFactory(factory)

    factory = connectTimeoutFactory(factory, timer)

    // We maintain a separate log of factory failures here so that
    // factory failures are captured in the service failure
    // stats. Logically these are service failures, but since the
    // requests are never dispatched to the underlying stack, they
    // don't get recorded there.
    factory = new StatsFactoryWrapper(factory, statsReceiver)
    factory = tracingFilter(tracer) andThen factory
    factory = codec.prepareServiceFactory(factory)

    factory
  }

  private[this] def internalBuildFactory(
    tracer: Tracer,
    timer: TwoTimer
  ) = new Managed[ServiceFactory[Req, Rep]] {
    def make() = new Disposable[ServiceFactory[Req, Rep]] {
      val inner = rawInternalBuildFactory(tracer, timer)
      def get =  inner
      def dispose(deadline: Time) = {
        inner.close()
        Future.value(())
      }
    }
  }

  def buildManagedFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): Managed[ServiceFactory[Req, Rep]] = for {
      timer <- ManagedTimer
      tracer <- config.tracerFactory
      factory <- internalBuildFactory(tracer, timer)
    } yield exceptionSourceFilter andThen factory

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols
   * (e.g., those that support transactions or authentication).
   */
  def buildFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): ServiceFactory[Req, Rep] = {
    val factory = buildManagedFactory()
    new ServiceFactory[Req, Rep] {
      val inner = factory.make()
      def apply(conn: ClientConnection) = inner.get.apply(conn)
      def close() = inner.dispose()
      override def isAvailable = inner.get.isAvailable
     }
  }

  @deprecated("Used for ABI compat", "5.0.1")
  def buildFactory(
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): ServiceFactory[Req, Rep] = buildFactory()(
    new ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]{})

  def buildManaged()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): Managed[Service[Req, Rep]] = for {
    factory <- buildManagedFactory()
    timer <- ManagedTimer
  } yield {
    var service: Service[Req, Rep] = new FactoryToService[Req, Rep](factory)
    // We keep the retrying filter after the load balancer so we can
    // retry across different hosts rather than the same one repeatedly.
    service = retryFilter(timer) andThen service
    service = globalTimeoutFilter(timer) andThen service
    service = exceptionSourceFilter andThen service
    config.cluster match {
      case Some(cluster) if !cluster.ready.isDefined =>
        new ProxyService(cluster.ready.map { _ => service }, config.hostConnectionMaxWaiters getOrElse(Int.MaxValue))
      case _ => service
    }
  }

  /**
   * Construct a Service.
   */
  def build()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): Service[Req, Rep] = new Service[Req, Rep] {
      val inner = buildManaged().make()
      def apply(request: Req) = inner.get(request)
      override def isAvailable = inner.get.isAvailable
      override def release() = inner.dispose()
    }

  @deprecated("Used for ABI compat", "5.0.1")
  def build(
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Service[Req, Rep] = build()(
    new ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]{})

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

  protected def failureAccrualFactory(factory: ServiceFactory[Req, Rep], timer: TwoTimer) =
    config.failureAccrual map { _(timer.toTwitterTimer) andThen factory } getOrElse factory

  protected def monitorFilter =
    config.monitor map { monitorFactory =>
      val name = config.name.get
      val logger = config.logger.getOrElse(Logger.getLogger(name))
      new MonitorFilter[Req, Rep](monitorFactory(name) andThen new SourceTrackingMonitor(logger, "client"))
    } getOrElse(identityFilter)

  protected def connectTimeoutFactory(factory: ServiceFactory[Req, Rep], timer: TwoTimer) =
    if (config.connectTimeout < Duration.MaxValue) {
      val exception = new ServiceTimeoutException(config.connectTimeout)
      new TimeoutFactory(factory, config.connectTimeout, exception, timer.toTwitterTimer)
    } else {
      factory
    }

  protected def exceptionSourceFilter = new ExceptionSourceFilter[Req, Rep](config.name.get)

  protected def retryFilter(timer: TwoTimer) =
    config.retryPolicy map { retryPolicy =>
      new RetryingFilter[Req, Rep](retryPolicy, timer.toTwitterTimer, statsReceiver)
    } getOrElse(identityFilter)


  protected def requestTimeoutFilter(timer: TwoTimer) =
    if (config.requestTimeout < Duration.MaxValue) {
      val exception = new IndividualRequestTimeoutException(config.requestTimeout)
      new TimeoutFilter[Req, Rep](config.requestTimeout, exception, timer.toTwitterTimer)
    } else {
      identityFilter
    }

  protected def globalTimeoutFilter(timer: TwoTimer) =
    if (config.timeout < Duration.MaxValue) {
      val exception = new GlobalRequestTimeoutException(config.timeout)
      new TimeoutFilter[Req, Rep](config.timeout, exception, timer.toTwitterTimer)
    } else {
      identityFilter
    }

  protected def tracingFilter(tracer: Tracer) = new TracingFilter[Req, Rep](tracer)

  protected val identityFilter = Filter.identity[Req, Rep]
}
