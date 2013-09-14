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

import com.twitter.finagle._
import com.twitter.finagle.filter.ExceptionSourceFilter
import com.twitter.finagle.loadbalancer.{LoadBalancerFactory, HeapBalancerFactory}
import com.twitter.finagle.netty3.ChannelSnooper
import com.twitter.finagle.service.{FailureAccrualFactory, ProxyService,
  RetryPolicy, RetryingFilter, TimeoutFilter}
import com.twitter.finagle.ssl.{Engine, Ssl}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{NullTracer, Tracer}
import com.twitter.finagle.util._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Duration, Future, Monitor, NullMonitor, Time, Timer, Try}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Logger, Level}
import javax.net.ssl.SSLContext
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipeline}
import scala.annotation.implicitNotFound
import scala.collection.mutable

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
}

object ClientConfig {
  sealed abstract trait Yes
  type FullySpecified[Req, Rep] = ClientConfig[Req, Rep, Yes, Yes, Yes]
}

@implicitNotFound("Builder is not fully configured: Cluster: ${HasCluster}, Codec: ${HasCodec}, HostConnectionLimit: ${HasHostConnectionLimit}")
private[builder] trait ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]

private[builder] object ClientConfigEvidence {
  implicit object FullyConfigured extends ClientConfigEvidence[ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]
}

// Necessary because of the 22 argument limit on case classes
private[builder] final case class ClientHostConfig(
  private val _hostConnectionCoresize    : Option[Int]                   = None,
  private val _hostConnectionLimit       : Option[Int]                   = None,
  private val _hostConnectionIdleTime    : Option[Duration]              = None,
  private val _hostConnectionMaxWaiters  : Option[Int]                   = None,
  private val _hostConnectionMaxIdleTime : Option[Duration]              = None,
  private val _hostConnectionMaxLifeTime : Option[Duration]              = None,
  private val _hostConnectionBufferSize  : Option[Int]                   = None) {
  val hostConnectionCoresize    = _hostConnectionCoresize
  val hostConnectionLimit       = _hostConnectionLimit
  val hostConnectionIdleTime    = _hostConnectionIdleTime
  val hostConnectionMaxWaiters  = _hostConnectionMaxWaiters
  val hostConnectionMaxIdleTime = _hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConnectionMaxLifeTime
  val hostConnectionBufferSize  = _hostConnectionBufferSize
}

private[builder] final case class StatsReceiverConfig(
  statsReceiver     : Option[StatsReceiver] = None,
  hostStatsReceiver : Option[StatsReceiver] = None
)

private[builder] final case class ClientTimeoutConfig(
  private val _tcpConnectTimeout: Duration         = 10.milliseconds,
  private val _connectTimeout   : Duration         = Duration.Top,
  private val _requestTimeout   : Duration         = Duration.Top,
  private val _timeout          : Duration         = Duration.Top,
  private val _readerIdleTimeout: Option[Duration] = None,
  private val _writerIdleTimeout: Option[Duration] = None
) {
  val tcpConnectTimeout         = _tcpConnectTimeout
  val requestTimeout            = _requestTimeout
  val connectTimeout            = _connectTimeout
  val timeout                   = _timeout
  val readerIdleTimeout         = _readerIdleTimeout
  val writerIdleTimeout         = _writerIdleTimeout
}


/**
 * TODO: do we really need to specify HasCodec? -- it's implied in a
 * way by the proper Req, Rep.
 *
 * Note: these are documented in ClientBuilder, as that is where they
 * are accessed by the end-user.
 */
private[builder] final case class ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit](
  private val _group                     : Option[Group[SocketAddress]]        = None,
  private val _loadBalancer              : Option[LoadBalancerFactory]   = None,
  private val _codecFactory              : Option[CodecFactory[Req, Rep]#Client] = None,
  private val _keepAlive                 : Option[Boolean]               = None,
  private val _statsReceiverConfig       : StatsReceiverConfig           = StatsReceiverConfig(),
  private val _monitor                   : Option[String => Monitor]     = None,
  private val _name                      : String                        = "client",
  private val _sendBufferSize            : Option[Int]                   = None,
  private val _recvBufferSize            : Option[Int]                   = None,
  private val _retryPolicy               : Option[RetryPolicy[Try[Nothing]]]  = None,
  private val _logger                    : Option[Logger]                = None,
  private val _channelFactory            : Option[ChannelFactory]        = None,
  private val _tls                       : Option[(() => Engine, Option[String])] = None,
  private val _httpProxy                 : Option[SocketAddress]          = None,
  private val _socksProxy                : Option[SocketAddress]          = None,
  private val _failureAccrual            : Option[Timer => ServiceFactoryWrapper] = Some(FailureAccrualFactory.wrapper(5, 5.seconds)),
  private val _tracer                    : Tracer                        = NullTracer,
  private val _hostConfig                : ClientHostConfig              = new ClientHostConfig,
  private val _failFast                  : Boolean                       = true,
  private val _timeoutConfig             : ClientTimeoutConfig           = new ClientTimeoutConfig,
  private val _daemon                    : Boolean                       = false)
{
  import ClientConfig._

  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  val group                     = _group
  val loadBalancer              = _loadBalancer
  val codecFactory              = _codecFactory
  val statsReceiver             = _statsReceiverConfig.statsReceiver
  val hostStatsReceiver         = _statsReceiverConfig.hostStatsReceiver
  val tcpConnectTimeout         = _timeoutConfig.tcpConnectTimeout
  val requestTimeout            = _timeoutConfig.requestTimeout
  val connectTimeout            = _timeoutConfig.connectTimeout
  val timeout                   = _timeoutConfig.timeout
  val readerIdleTimeout         = _timeoutConfig.readerIdleTimeout
  val writerIdleTimeout         = _timeoutConfig.writerIdleTimeout
  val timeoutConfig             = _timeoutConfig
  val monitor                   = _monitor
  val keepAlive                 = _keepAlive
  val name                      = _name
  val hostConnectionCoresize    = _hostConfig.hostConnectionCoresize
  val hostConnectionLimit       = _hostConfig.hostConnectionLimit
  val hostConnectionIdleTime    = _hostConfig.hostConnectionIdleTime
  val hostConnectionMaxWaiters  = _hostConfig.hostConnectionMaxWaiters
  val hostConnectionMaxIdleTime = _hostConfig.hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConfig.hostConnectionMaxLifeTime
  val hostConnectionBufferSize  = _hostConfig.hostConnectionBufferSize
  val hostConfig                = _hostConfig
  val sendBufferSize            = _sendBufferSize
  val recvBufferSize            = _recvBufferSize
  val retryPolicy               = _retryPolicy
  val logger                    = _logger
  val channelFactory            = _channelFactory
  val tls                       = _tls
  val httpProxy                 = _httpProxy
  val socksProxy                = _socksProxy
  val failureAccrual            = _failureAccrual
  val tracer                    = _tracer
  val failFast                  = _failFast
  val daemon                    = _daemon

  def toMap = Map(
    "group"                     -> _group,
    "loadBalancer"              -> _loadBalancer,
    "codecFactory"              -> _codecFactory,
    "tcpConnectTimeout"         -> Some(_timeoutConfig.tcpConnectTimeout),
    "requestTimeout"            -> Some(_timeoutConfig.requestTimeout),
    "connectTimeout"            -> Some(_timeoutConfig.connectTimeout),
    "timeout"                   -> Some(_timeoutConfig.timeout),
    "keepAlive"                 -> Some(_keepAlive),
    "statsReceiver"             -> _statsReceiverConfig.statsReceiver,
    "hostStatsReceiver"         -> _statsReceiverConfig.hostStatsReceiver,
    "readerIdleTimeout"         -> _timeoutConfig.readerIdleTimeout,
    "writerIdleTimeout"         -> _timeoutConfig.writerIdleTimeout,
    "monitor"                   -> _monitor,
    "name"                      -> Some(_name),
    "hostConnectionCoresize"    -> _hostConfig.hostConnectionCoresize,
    "hostConnectionLimit"       -> _hostConfig.hostConnectionLimit,
    "hostConnectionIdleTime"    -> _hostConfig.hostConnectionIdleTime,
    "hostConnectionMaxWaiters"  -> _hostConfig.hostConnectionMaxWaiters,
    "hostConnectionMaxIdleTime" -> _hostConfig.hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime" -> _hostConfig.hostConnectionMaxLifeTime,
    "hostConnectionBufferSize"  -> _hostConfig.hostConnectionBufferSize,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "retryPolicy"               -> _retryPolicy,
    "logger"                    -> _logger,
    "channelFactory"            -> _channelFactory,
    "tls"                       -> _tls,
    "httpProxy"                 -> _httpProxy,
    "socksProxy"                -> _socksProxy,
    "failureAccrual"            -> _failureAccrual,
    "tracer"                    -> Some(_tracer),
    "failFast"                  -> failFast,
    "daemon"                    -> daemon
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
    group        getOrElse { throw new IncompleteSpecification("No hosts were specified") }
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
    withConfig(_.copy(_group = Some(Group.fromCluster(cluster))))

  def group(
    group: Group[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    withConfig(_.copy(_group = Some(group)))

  /**
   * Specify a load balancer.  The load balancer implements
   * a strategy for choosing one from a set of hosts to service a request
   */
  def loadBalancer(loadBalancer: LoadBalancerFactory): This =
    withConfig(_.copy(_loadBalancer = Some(loadBalancer)))

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
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_tcpConnectTimeout = duration)))

  /**
   * The request timeout is the time given to a *single* request (if
   * there are retries, they each get a fresh request timeout).  The
   * timeout is applied only after a connection has been acquired.
   * That is: it is applied to the interval between the dispatch of
   * the request and the receipt of the response.
   */
  def requestTimeout(duration: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_requestTimeout = duration)))

  /**
   * The connect timeout is the timeout applied to the acquisition of
   * a Service.  This includes both queueing time (eg.  because we
   * cannot create more connections due to {{hostConnectionLimit}} and
   * there are more than {{hostConnectionLimit}} requests outstanding)
   * as well as physical connection time.  Futures returned from
   * {{factory()}} will always be satisfied within this timeout.
   */
  def connectTimeout(duration: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_connectTimeout = duration)))

  /**
   * Total request timeout.  This timeout is applied from the issuance
   * of a request (through {{service(request)}}) until the
   * satisfaction of that reply future.  No request will take longer
   * than this.
   *
   * Applicable only to service-builds ({{build()}})
   */
  def timeout(duration: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_timeout = duration)))

  /**
   * Apply TCP keepAlive ({{SO_KEEPALIVE}} socket option).
   */
  def keepAlive(value: Boolean): This =
    withConfig(_.copy(_keepAlive = Some(value)))

  /**
   * The maximum time a connection may have received no data.
   */
  def readerIdleTimeout(duration: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_readerIdleTimeout = Some(duration))))

  /**
   * The maximum time a connection may not have sent any data.
   */
  def writerIdleTimeout(duration: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(_writerIdleTimeout = Some(duration))))

  /**
   * Report stats to the given {{StatsReceiver}}.  This will report
   * verbose global statistics and counters, that in turn may be
   * exported to monitoring applications.
   * NB: per hosts statistics will *NOT* be exported to this receiver
   *     @see reportHostStats(receiver: StatsReceiver)
   */
  def reportTo(receiver: StatsReceiver): This =
    withConfig(cfg => cfg.copy(_statsReceiverConfig = StatsReceiverConfig(Some(receiver), cfg.hostStatsReceiver)))

  /**
   * Report per host stats to the given {{StatsReceiver}}.
   * The statsReceiver will be scoped per client, like this:
   * client/connect_latency_ms_max/0.0.0.0:64754
   */
  def reportHostStats(receiver: StatsReceiver): This =
    withConfig(cfg => cfg.copy(_statsReceiverConfig = StatsReceiverConfig(cfg.statsReceiver, Some(receiver))))

  /**
   * Give a meaningful name to the client. Required.
   */
  def name(value: String): This = withConfig(_.copy(_name = value))

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
   * Experimental option to buffer `size` connections from the pool.
   * The buffer is fast and lock-free, reducing contention for
   * services with very high requests rates. The buffer size should
   * be sized roughly to the expected concurrency. Buffers sized by
   * power-of-twos may be faster due to the use of modular
   * arithmetic.
   *
   * '''Note:''' This will be integrated into the mainline pool, at
   * which time the experimental option will go away.
   */
  def expHostConnectionBufferSize(size: Int): This =
    withConfig(c => c.copy(_hostConfig =  c.hostConfig.copy(_hostConnectionBufferSize = Some(size))))

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
  def channelFactory(cf: ChannelFactory): This =
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
   * Make connections via the given HTTP proxy.
   * If this is defined concurrently with socksProxy, the order in which they are applied is undefined.
   */
  def httpProxy(httpProxy: SocketAddress): This =
    withConfig(_.copy(_httpProxy = Some(httpProxy)))

  /**
   * Make connections via the given SOCKS proxy.
   * If this is defined concurrently with httpProxy, the order in which they are applied is undefined.
   */
  def socksProxy(socksProxy: SocketAddress): This =
    withConfig(_.copy(_socksProxy = Some(socksProxy)))

  /**
   * Specifies a tracer that receives trace events.
   * See [[com.twitter.finagle.tracing]] for details.
   */
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(factory: Tracer.Factory): This =
    tracer(factory())

  // API compatibility method
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(t: Tracer): This =
    tracer(t)

  /**
   * Specifies a tracer that receives trace events.
   * See [[com.twitter.finagle.tracing]] for details.
   */
  def tracer(t: Tracer): This =
    withConfig(_.copy(_tracer = t))

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

  @deprecated(
    "No longer experimental: Use failFast()." +
    "The new default value is true, so replace .expFailFast(true) with nothing at all",
    "5.3.10")
  def expFailFast(onOrOff: Boolean): This =
    failFast(onOrOff)

  /**
   * Marks a host dead on connection failure. The host remains dead
   * until we succesfully connect.
   *
   * Intermediate connection attempts *are* respected, but host
   * availability is turned off during the reconnection period.
   */
  def failFast(onOrOff: Boolean): This =
    withConfig(_.copy(_failFast = onOrOff))

  /**
   * When true, the client is daemonized. As with java threads, a
   * process can exit only when all remaining clients are daemonized.
   * False by default.
   */
  def daemon(daemonize: Boolean): This =
    withConfig(_.copy(_daemon = daemonize))

  /*** BUILD ***/

  private[finagle] lazy val statsReceiver =
    (config.statsReceiver getOrElse NullStatsReceiver).scope(config.name)

  private[finagle] lazy val hostStatsReceiver =
    config.hostStatsReceiver map(_.scope(config.name)) getOrElse statsReceiver

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols
   * (e.g., those that support transactions or authentication).
   */
  def buildFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): ServiceFactory[Req, Rep] = {
    val codec = config.codecFactory.get(ClientCodecConfig(serviceName = config.name))

    // We configure a client based on the parameters of the client
    // builder. TODO: this should be moved to its own toplevel class
    // that we can unittest independently.
    import com.twitter.finagle.client._
    import com.twitter.finagle.netty3.{Netty3Transporter, Netty3TransporterTLSConfig}

    val tracer = config.tracer
    val timer = DefaultTimer.twitter
    val nettyTimer = DefaultTimer
    val monitor = config.monitor map { newMonitor => newMonitor(config.name) } getOrElse NullMonitor
    val logger = config.logger getOrElse Logger.getLogger(config.name)

    val newChannel: ChannelPipeline => Channel = {
      val factory = config.channelFactory getOrElse Netty3Transporter.channelFactory
      factory.newChannel(_)
    }

    // The transporter connects to actual endpoints, providing a typed
    // session transport.
    val transporter = Netty3Transporter[Req, Rep](
      name = config.name,
      pipelineFactory = codec.pipelineFactory,
      newChannel = newChannel,
      newTransport = codec.newClientTransport(_, statsReceiver),
      tlsConfig = config.tls map { case (e, v) => Netty3TransporterTLSConfig(e, v) },
      httpProxy = config.httpProxy,
      socksProxy = config.socksProxy,
      channelReaderTimeout = config.readerIdleTimeout getOrElse Duration.Top,
      channelWriterTimeout = config.writerIdleTimeout getOrElse Duration.Top,
      channelSnooper = config.logger map { log => ChannelSnooper(config.name)(log.log(Level.INFO, _, _)) },
      channelOptions = {
        val o = new mutable.MapBuilder[String, Object, Map[String, Object]](Map())
        o += "connectTimeoutMillis" -> (config.tcpConnectTimeout.inMilliseconds: java.lang.Long)
        o += "tcpNoDelay" -> java.lang.Boolean.TRUE
        o += "reuseAddress" -> java.lang.Boolean.TRUE
        for (v <- config.keepAlive) o += "keepAlive" -> (v: java.lang.Boolean)
        for (s <- config.sendBufferSize) o += "sendBufferSize" -> (s: java.lang.Integer)
        for (s <- config.recvBufferSize) o += "receiveBufferSize" -> (s: java.lang.Integer)
        o.result()
      }
    )

    val measureConn: Transformer[Req, Rep] = underlying =>
      new ServiceFactoryProxy(underlying) {
        // TODO: this stat should really be rolled up
        val stat = statsReceiver.stat("codec_connection_preparation_latency_ms")
        override def apply(conn: ClientConnection) = {
          val begin = Time.now
          super.apply(conn) ensure {
            stat.add((Time.now - begin).inMilliseconds)
          }
        }
      }

    val prepareConn: Transformer[Req, Rep] =
      measureConn compose (codec.prepareConnFactory _)

    val endpointer: (SocketAddress, StatsReceiver) => ServiceFactory[Req, Rep] = {
      val bridged = Bridge[Req, Rep, Req, Rep](transporter, codec.newClientDispatcher(_))
      (addr, statsReceiver) => prepareConn(bridged(addr, statsReceiver))
    }

    val pool = DefaultPool[Req, Rep](
      low = config.hostConnectionCoresize getOrElse 1,
      high = Seq(config.hostConnectionCoresize getOrElse 1,
        config.hostConnectionLimit getOrElse Int.MaxValue).max,
      bufferSize = config.hostConnectionBufferSize getOrElse 0,
      idleTime = config.hostConnectionIdleTime getOrElse 5.seconds,
      maxWaiters = config.hostConnectionMaxWaiters getOrElse Int.MaxValue,
      timer = timer
    )

    // The client puts it all together: given a binder, a pool and
    // other paramters, it composes a load-balanced client.
    val client = DefaultClient[Req, Rep](
      name = config.name,
      endpointer = endpointer,
      pool = pool,
      maxIdletime = config.hostConnectionMaxIdleTime getOrElse Duration.Top,
      maxLifetime = config.hostConnectionMaxLifeTime getOrElse Duration.Top,
      requestTimeout = config.requestTimeout,
      failureAccrual = {
        val wrapper = config.failureAccrual map { newFailureAccrual =>
            newFailureAccrual(timer) } getOrElse ServiceFactoryWrapper.identity
        wrapper.andThen(_)
      },
      failFast = config.failFast && codec.failFastOk,
      serviceTimeout = config.connectTimeout,
      timer = timer,
      statsReceiver = statsReceiver,
      hostStatsReceiver = hostStatsReceiver,
      tracer = tracer,
      monitor = monitor,
      reporter = NullReporterFactory,
      loadBalancerFactory = config.loadBalancer getOrElse HeapBalancerFactory
    )

    // Note the direct use of newStack here. This is because we want
    // to control how stats receivers are scoped.
    val factory = codec.prepareServiceFactory(
      client.newStack(config.group.get))

    if (!config.daemon) ExitGuard.guard()
    new ServiceFactoryProxy[Req, Rep](factory) {
      private[this] val closed = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!closed.compareAndSet(false, true)) {
          logger.log(Level.WARNING, "Close on ServiceFactory called multiple times!",
            new Exception/*stack trace please*/)
          return Future.exception(new IllegalStateException)
        }

        super.close(deadline) ensure {
          if (!config.daemon) ExitGuard.unguard()
        }
      }
    }
  }

  @deprecated("Used for ABI compat", "5.0.1")
  def buildFactory(
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): ServiceFactory[Req, Rep] = buildFactory()(
    new ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]{})

  /**
   * Construct a Service.
   */
  def build()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): Service[Req, Rep] = {
    val underlying: Service[Req, Rep] = new FactoryToService[Req, Rep](buildFactory())
    val service = config.group match {
      case Some(group) if group.members.isEmpty =>
        new ProxyService(Group.pollUntilTrue[SocketAddress](group, !_.members.isEmpty) map Function.const(underlying),
          config.hostConnectionMaxWaiters getOrElse Int.MaxValue)
      case _ => underlying
    }

    val timer = DefaultTimer.twitter

    // We keep the retrying filter after the load balancer so we can
    // retry across different hosts rather than the same one repeatedly.
    val filter = exceptionSourceFilter andThen globalTimeoutFilter(timer) andThen retryFilter(timer)
    val composed = filter andThen service

    new ServiceProxy[Req, Rep](composed) {
      private[this] val released = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!released.compareAndSet(false, true)) {
          val logger = config.logger getOrElse Logger.getLogger(config.name)
          logger.log(Level.WARNING, "Release on Service called multiple times!",
            new Exception/*stack trace please*/)
          return Future.exception(new IllegalStateException)
        }
        super.close(deadline)
      }
    }
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

  private def exceptionSourceFilter = new ExceptionSourceFilter[Req, Rep](config.name)

  private def retryFilter(timer: Timer) =
    config.retryPolicy map { retryPolicy =>
      new RetryingFilter[Req, Rep](retryPolicy, timer, statsReceiver)
    } getOrElse(identityFilter)

  private def globalTimeoutFilter(timer: Timer) =
    if (config.timeout < Duration.Top) {
      val exception = new GlobalRequestTimeoutException(config.timeout)
      new TimeoutFilter[Req, Rep](config.timeout, exception, timer)
    } else {
      identityFilter
    }

  private val identityFilter = Filter.identity[Req, Rep]
}
