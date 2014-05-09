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
import com.twitter.finagle.client.{DefaultPool, StackClient, Transporter}
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.filter.ExceptionSourceFilter
import com.twitter.finagle.loadbalancer.{LoadBalancerFactory, WeightedLoadBalancerFactory}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.service._
import com.twitter.finagle.ssl.{Engine, Ssl}
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.{NullStatsReceiver, ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Duration, Future, NullMonitor, Time, Var, Try}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import javax.net.ssl.SSLContext
import org.jboss.netty.channel.{Channel, ChannelFactory}
import scala.annotation.implicitNotFound

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
  val DefaultName = "client"

  def nilClient[Req, Rep] = new StackClient[Req, Rep, Any, Any](nilStack, Stack.Params.empty) {
    val newTransporter: Stack.Params => Transporter[Any, Any] =
      Function.const(new Transporter[Any, Any] {
        def apply(sa: SocketAddress) = Future.exception(new Exception("unimplemented"))
      })

    val newDispatcher: Stack.Params => Dispatcher = { _ =>
      trans => Service.mk[Req, Rep](Function.const(
        Future.exception(new Exception("unimplemented"))
      ))
    }
  }

  // params specific to ClientBuilder
  case class DestName(name: Name)
  implicit object DestName extends Stack.Param[DestName] {
    val default = DestName(Name.empty)
  }

  case class GlobalTimeout(timeout: Duration)
  implicit object GlobalTimeout extends Stack.Param[GlobalTimeout] {
    val default = GlobalTimeout(Duration.Top)
  }

  case class Retries(policy: RetryPolicy[Try[Nothing]])
  implicit object Retries extends Stack.Param[Retries] {
    private[this] val none = new RetryPolicy[Try[Nothing]] {
      def apply(t: Try[Nothing]) = None
    }
    val default = Retries(none)
  }

  case class Daemonize(onOrOff: Boolean)
  implicit object Daemonize extends Stack.Param[Daemonize] {
    val default = Daemonize(true)
  }

  case class FailureAccrualFac(fac: com.twitter.util.Timer => ServiceFactoryWrapper)
  implicit object FailureAccrualFac extends Stack.Param[FailureAccrualFac] {
    val default = FailureAccrualFac(Function.const(ServiceFactoryWrapper.identity)_)
  }

  case class FailFast(onOrOff: Boolean)
  implicit object FailFast extends Stack.Param[FailFast] {
    val default = FailFast(true)
  }

  case class MonitorFactory(mFactory: String => com.twitter.util.Monitor)
  implicit object MonitorFactory extends Stack.Param[MonitorFactory] {
    val default = MonitorFactory(_ => NullMonitor)
  }

  // historical defaults for ClientBuilder
  val DefaultParams = Stack.Params.empty +
    param.Stats(NullStatsReceiver) +
    param.Label(DefaultName) +
    DefaultPool.Param(low = 1, high = Int.MaxValue,
    bufferSize = 0, idleTime = 5.seconds, maxWaiters = Int.MaxValue) +
    param.Tracer(NullTracer) +
    param.Monitor(NullMonitor) +
    param.Reporter(NullReporterFactory) +
    Daemonize(false)
}

@implicitNotFound("Builder is not fully configured: Cluster: ${HasCluster}, Codec: ${HasCodec}, HostConnectionLimit: ${HasHostConnectionLimit}")
private[builder] trait ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]

private[builder] object ClientConfigEvidence {
  implicit object FullyConfigured extends ClientConfigEvidence[ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]
}

/**
 * TODO: do we really need to specify HasCodec? -- it's implied in a
 * way by the proper Req, Rep.
 *
 * Note: these are documented in ClientBuilder, as that is where they
 * are accessed by the end-user.
 */
private[builder] final class ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]

class ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit] private[finagle](
  params: Stack.Params,
  mk: Stack.Params => StackClient[Req, Rep, Any, Any]
) {
  import ClientConfig._
  import com.twitter.finagle.param._

  // Convenient aliases.
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
  type ThisConfig           = ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]
  type This                 = ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]

  private[builder] def this() = this(ClientConfig.DefaultParams, Function.const(ClientConfig.nilClient)_)

  override def toString() = "ClientBuilder(%s)".format(params)

  protected def copy[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    ps: Stack.Params,
    newClient: Stack.Params => StackClient[Req1, Rep1, Any, Any]
  ): ClientBuilder[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1] =
    new ClientBuilder(ps, newClient)

  protected def configured[P: Stack.Param, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    param: P
  ): ClientBuilder[Req, Rep, HasCluster1, HasCodec1, HasHostConnectionLimit1] =
    copy(params + param, mk)

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
    addrs: Seq[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    dest(Name.bound(addrs:_*))

  /**
   * A convenience method for specifying a one-host
   * [[java.net.SocketAddress]] client.
   */
  def hosts(
    address: SocketAddress
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    hosts(Seq(address))

  /**
   * The logical destination of requests dispatched through this
   * client, as evaluated by a resolver. If the name evaluates a
   * label, this replaces the builder's current name.
   */
  def dest(
    addr: String
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] = {
    Resolver.evalLabeled(addr) match {
      case (n, "") => dest(n)
      case (n, l) =>
        val Label(label) = params[Label]
        val cb =
          if (label.isEmpty || l != addr)
            this.name(l)
          else
            this

        cb.dest(n)
    }
  }

  /**
   * The logical destination of requests dispatched through this
   * client.
   */
  def dest(
    name: Name
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    configured(DestName(name))

  /**
   * Specify a cluster directly.  A
   * [[com.twitter.finagle.builder.Cluster]] defines a dynamic
   * mechanism for specifying a set of endpoints to which this client
   * remains connected.
   */
  def cluster(
    cluster: Cluster[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    group(Group.fromCluster(cluster))

  def group(
    group: Group[SocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    dest(Name.fromGroup(group))

  def loadBalancer(lbf: LoadBalancerFactory): This =
    loadBalancer(lbf.toWeighted)

  /**
   * Specify a load balancer.  The load balancer implements
   * a strategy for choosing one from a set of hosts to service a request
   */
  def loadBalancer(loadBalancer: WeightedLoadBalancerFactory): This =
    configured(LoadBalancerFactory.Param(loadBalancer))

  /**
   * Specify the codec. The codec implements the network protocol
   * used by the client, and consequently determines the {{Req}} and {{Rep}}
   * type variables. One of the codec variations is required.
   */
  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(Function.const(codec)(_))

  /**
   * A variation of {{codec}} that supports codec factories.  This is
   * used by codecs that need dynamic construction, but should be
   * transparent to the user.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(codecFactory.client)

  /**
   * A variation of codec for codecs that support only client-codecs.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Client
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    stack({ ps =>
      val Label(name) = ps[Label]
      val codec = codecFactory(ClientCodecConfig(name))

      val prepConn = new Stack.Simple[ServiceFactory[Req1, Rep1]](StackClient.Role.PrepConn) {
        def make(params: Params, next: ServiceFactory[Req1, Rep1]) = {
          val Stats(stats) = params[Stats]
          val underlying = codec.prepareConnFactory(next)
          new ServiceFactoryProxy(underlying) {
            val stat = stats.stat("codec_connection_preparation_latency_ms")
            override def apply(conn: ClientConnection) = {
              val begin = Time.now
              super.apply(conn) ensure {
                stat.add((Time.now - begin).inMilliseconds)
              }
            }
          }
        }
      }

      val newStack = {
        val stk = StackClient.newStack[Req1, Rep1]
          .replace(StackClient.Role.PrepConn, prepConn)
          .replace(StackClient.Role.PrepFactory, (next: ServiceFactory[Req1, Rep1]) =>
            codec.prepareServiceFactory(next))

        // respect the codec's request to disable failfast
        if (!codec.failFastOk) stk.remove(FailFastFactory.FailFast)
        else stk
      }

      new StackClient[Req1, Rep1, Any, Any](newStack, ps) {
        protected val newTransporter: Stack.Params => Transporter[Any, Any] = { prms =>
          val Stats(stats) = prms[Stats]
          val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
          Netty3Transporter[Any, Any](codec.pipelineFactory,
            prms + Netty3Transporter.TransportFactory(newTransport))
        }

        protected val newDispatcher: Stack.Params => Dispatcher = { _ =>
          trans => codec.newClientDispatcher(trans)
        }
      }
    })

  def stack[Req1, Rep1](
    mk: Stack.Params => StackClient[Req1, Rep1, Any, Any]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    copy(params, mk)

  @deprecated("Use tcpConnectTimeout instead", "5.0.1")
  def connectionTimeout(duration: Duration): This = tcpConnectTimeout(duration)

  /**
   * Specify the TCP connection timeout.
   */
  def tcpConnectTimeout(duration: Duration): This =
    configured(Transporter.ConnectTimeout(duration))

  /**
   * The request timeout is the time given to a *single* request (if
   * there are retries, they each get a fresh request timeout).  The
   * timeout is applied only after a connection has been acquired.
   * That is: it is applied to the interval between the dispatch of
   * the request and the receipt of the response.
   */
  def requestTimeout(duration: Duration): This =
    configured(TimeoutFilter.Param(duration))

  /**
   * The connect timeout is the timeout applied to the acquisition of
   * a Service.  This includes both queueing time (eg.  because we
   * cannot create more connections due to {{hostConnectionLimit}} and
   * there are more than {{hostConnectionLimit}} requests outstanding)
   * as well as physical connection time.  Futures returned from
   * {{factory()}} will always be satisfied within this timeout.
   */
  def connectTimeout(duration: Duration): This =
    configured(TimeoutFactory.Param(duration))

  /**
   * Total request timeout.  This timeout is applied from the issuance
   * of a request (through {{service(request)}}) until the
   * satisfaction of that reply future.  No request will take longer
   * than this.
   *
   * Applicable only to service-builds ({{build()}})
   */
  def timeout(duration: Duration): This =
    configured(GlobalTimeout(duration))

  /**
   * Apply TCP keepAlive ({{SO_KEEPALIVE}} socket option).
   */
  def keepAlive(value: Boolean): This =
    configured(params[Transport.Liveness].copy(keepAlive = Some(value)))

  /**
   * The maximum time a connection may have received no data.
   */
  def readerIdleTimeout(duration: Duration): This =
    configured(params[Transport.Liveness].copy(readTimeout = duration))

  /**
   * The maximum time a connection may not have sent any data.
   */
  def writerIdleTimeout(duration: Duration): This =
    configured(params[Transport.Liveness].copy(writeTimeout = duration))

  /**
   * Report stats to the given {{StatsReceiver}}.  This will report
   * verbose global statistics and counters, that in turn may be
   * exported to monitoring applications.
   * NB: per hosts statistics will *NOT* be exported to this receiver
   *     @see reportHostStats(receiver: StatsReceiver)
   */
  def reportTo(receiver: StatsReceiver): This =
    configured(Stats(receiver))

  /**
   * Report per host stats to the given {{StatsReceiver}}.
   * The statsReceiver will be scoped per client, like this:
   * client/connect_latency_ms_max/0.0.0.0:64754
   */
  def reportHostStats(receiver: StatsReceiver): This =
    configured(LoadBalancerFactory.HostStats(receiver))

  /**
   * Give a meaningful name to the client. Required.
   */
  def name(value: String): This =
    configured(Label(value))

  /**
   * The maximum number of connections that are allowed per host.
   * Required.  Finagle guarantees to never have more active
   * connections than this limit.
   */
  def hostConnectionLimit(value: Int): ClientBuilder[Req, Rep, HasCluster, HasCodec, Yes] =
    configured(params[DefaultPool.Param].copy(high = value))

  /**
   * The core size of the connection pool: the pool is not shrinked below this limit.
   */
  def hostConnectionCoresize(value: Int): This =
    configured(params[DefaultPool.Param].copy(low = value))

  /**
   * The amount of time a connection is allowed to linger (when it
   * otherwise would have been closed by the pool) before being
   * closed.
   */
  def hostConnectionIdleTime(timeout: Duration): This =
    configured(params[DefaultPool.Param].copy(idleTime = timeout))

  /**
   * The maximum queue size for the connection pool.
   */
  def hostConnectionMaxWaiters(nWaiters: Int): This =
    configured(params[DefaultPool.Param].copy(maxWaiters = nWaiters))

  /**
   * The maximum time a connection is allowed to linger unused.
   */
  def hostConnectionMaxIdleTime(timeout: Duration): This =
    configured(params[ExpiringService.Param].copy(idleTime = timeout))

  /**
   * The maximum time a connection is allowed to exist, regardless of occupancy.
   */
  def hostConnectionMaxLifeTime(timeout: Duration): This =
    configured(params[ExpiringService.Param].copy(lifeTime = timeout))

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
    configured(params[DefaultPool.Param].copy(bufferSize = size))

  /**
   * The number of retries applied. Only applicable to service-builds ({{build()}})
   */
  def retries(value: Int): This =
    retryPolicy(RetryPolicy.tries(value))

  def retryPolicy(value: RetryPolicy[Try[Nothing]]): This =
    configured(Retries(value))

  /**
   * Sets the TCP send buffer size.
   */
  def sendBufferSize(value: Int): This =
    configured(params[Transport.BufferSizes].copy(send = Some(value)))

  /**
   * Sets the TCP recv buffer size.
   */
  def recvBufferSize(value: Int): This =
    configured(params[Transport.BufferSizes].copy(recv = Some(value)))

  /**
   * Use the given channel factory instead of the default. Note that
   * when using a non-default ChannelFactory, finagle can't
   * meaningfully reference count factory usage, and so the caller is
   * responsible for calling ``releaseExternalResources()''.
   */
  def channelFactory(cf: ChannelFactory): This =
    configured(Netty3Transporter.ChannelFactory(cf))

    /**
   * Encrypt the connection with SSL.  Hostname verification will be
   * provided against the given hostname.
   */
  def tls(hostname: String): This = {
    configured(Transport.TLSEngine(Some({ () => Ssl.client() })))
      .configured(Transporter.TLSHostname(Some(hostname)))
  }

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * No SSL Hostname Validation is performed
   */
  def tls(sslContext: SSLContext): This =
    configured(Transport.TLSEngine(Some({ () => Ssl.client(sslContext) })))

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * SSL Hostname Validation is performed, on the passed in hostname
   */
  def tls(sslContext: SSLContext, hostname: Option[String]): This = {
    configured((Transport.TLSEngine(Some({ () => Ssl.client(sslContext) }))))
      .configured(Transporter.TLSHostname(hostname))
  }

  /**
   * Do not perform TLS validation. Probably dangerous.
   */
  def tlsWithoutValidation(): This =
    configured(Transport.TLSEngine(Some({ () => Ssl.clientWithoutCertificateValidation() })))

  /**
   * Make connections via the given HTTP proxy.
   * If this is defined concurrently with socksProxy, the order in which they are applied is undefined.
   */
  def httpProxy(httpProxy: SocketAddress): This =
    configured(Transporter.HttpProxy(Some(httpProxy)))

  /**
   * Make connections via the given SOCKS proxy.
   * If this is defined concurrently with httpProxy, the order in which they are applied is undefined.
   */
  def socksProxy(socksProxy: SocketAddress): This =
    configured(params[Transporter.SocksProxy].copy(sa = Some(socksProxy)))

  /**
   * For the socks proxy use this username for authentication.
   * socksPassword and socksProxy must be set as well
   */
  def socksUsernameAndPassword(credentials: (String,String)): This =
    configured(params[Transporter.SocksProxy].copy(credentials = Some(credentials)))

  /**
   * Specifies a tracer that receives trace events.
   * See [[com.twitter.finagle.tracing]] for details.
   */
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(factory: com.twitter.finagle.tracing.Tracer.Factory): This =
    tracer(factory())

  // API compatibility method
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(t: com.twitter.finagle.tracing.Tracer): This =
    tracer(t)

  /**
   * Specifies a tracer that receives trace events.
   * See [[com.twitter.finagle.tracing]] for details.
   */
  def tracer(t: com.twitter.finagle.tracing.Tracer): This =
    configured(Tracer(t))

  def monitor(mFactory: String => com.twitter.util.Monitor): This =
    configured(MonitorFactory(mFactory))

  /**
   * Log very detailed debug information to the given logger.
   */
  def logger(logger: java.util.logging.Logger): This =
    configured(Logger(logger))

  /**
   * Use the given paramters for failure accrual.  The first parameter
   * is the number of *successive* failures that are required to mark
   * a host failed.  The second paramter specifies how long the host
   * is dead for, once marked.
   */
  def failureAccrualParams(pair: (Int, Duration)): This = {
    val (numFailures, markDeadFor) = pair
    failureAccrualFactory(FailureAccrualFactory.wrapper(numFailures, markDeadFor)(_))
  }

  def failureAccrual(failureAccrual: ServiceFactoryWrapper): This =
    failureAccrualFactory { _ => failureAccrual }

  def failureAccrualFactory(factory: com.twitter.util.Timer => ServiceFactoryWrapper): This =
    configured(FailureAccrualFac(factory))

  @deprecated(
    "No longer experimental: Use failFast()." +
    "The new default value is true, so replace .expFailFast(true) with nothing at all",
    "5.3.10")
  def expFailFast(onOrOff: Boolean): This =
    failFast(onOrOff)

  /**
   * Marks a host dead on connection failure. The host remains dead
   * until we succesfully connect. Intermediate connection attempts
   * *are* respected, but host availability is turned off during the
   * reconnection period.
   */
  def failFast(onOrOff: Boolean): This =
    configured(FailFast(onOrOff))

  /**
   * When true, the client is daemonized. As with java threads, a
   * process can exit only when all remaining clients are daemonized.
   * False by default.
   */
  def daemon(daemonize: Boolean): This =
    configured(Daemonize(daemonize))

  /*** BUILD ***/

  // still used by finagle-memcached.
  private[finagle] lazy val statsReceiver = {
    val Stats(sr) = params[Stats]
    val Label(label) = params[Label]
    sr.scope(label)
  }

  /**
   * Construct a ServiceFactory. This is useful for stateful protocols
   * (e.g., those that support transactions or authentication).
   */
  def buildFactory()(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ClientBuilder_DOCUMENTATION:
      ClientConfigEvidence[HasCluster, HasCodec, HasHostConnectionLimit]
  ): ServiceFactory[Req, Rep] = {
    import LoadBalancerFactory.HostStats

    val Label(label) = params[Label]
    val Daemonize(daemon) = params[Daemonize]
    val Logger(logger) = params[Logger]
    val Timer(timer) = params[Timer]
    val DestName(dest) = params[DestName]
    val MonitorFactory(mFactory) = params[MonitorFactory]
    val Stats(stats) = params[Stats]
    val HostStats(hostStats) = params[HostStats]

    // transform stack wrt. failure modules
    val client = mk(params) transformed { stack =>
      val stk = if (params.contains[FailureAccrualFac]) {
        val FailureAccrualFac(fac) = params[FailureAccrualFac]
        stack.replace(FailureAccrualFactory.FailureAccrual, (next: ServiceFactory[Req, Rep]) =>
          fac(timer) andThen next)
      } else {
        stack
      }

      val FailFast(failFast) = params[FailFast]
      if (!failFast) stk.remove(FailFastFactory.FailFast)
      else stk
    }

    // Note, we push the responsibility of scoping the statsReceivers to the client
    // implementations. We maintain historical behavior of enabling per host
    // stats if no host stats receiver was set.
    val hostStatsReceiver = if (params.contains[HostStats]) hostStats else stats

    val factory = client
      .configured(HostStats(hostStatsReceiver))
      .configured(Monitor(mFactory(label)))
      .newClient(dest, label)

    if (!daemon) ExitGuard.guard()
    new ServiceFactoryProxy[Req, Rep](factory) {
      private[this] val closed = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!closed.compareAndSet(false, true)) {
          logger.log(Level.WARNING, "Close on ServiceFactory called multiple times!",
            new Exception/*stack trace please*/)
          return Future.exception(new IllegalStateException)
        }

        super.close(deadline) ensure {
          if (!daemon) ExitGuard.unguard()
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
    val service: Service[Req, Rep] = new FactoryToService[Req, Rep](buildFactory())

    val Label(label) = params[Label]
    val Timer(timer) = params[Timer]

    val exceptionSourceFilter = new ExceptionSourceFilter[Req, Rep](label)
    // We keep the retrying filter after the load balancer so we can
    // retry across different hosts rather than the same one repeatedly.
    val filter = exceptionSourceFilter andThen globalTimeoutFilter(timer) andThen retryFilter(timer)
    val composed = filter andThen service

    new ServiceProxy[Req, Rep](composed) {
      private[this] val released = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!released.compareAndSet(false, true)) {
          val Logger(logger) = params[Logger]
          logger.log(java.util.logging.Level.WARNING, "Release on Service called multiple times!",
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

  private[this] def validated = {
    if (!params.contains[DestName])
      throw new IncompleteSpecification("No destination was specified")

    if (mk(params).stack.tails.size == 1)
      throw new IncompleteSpecification("No codec was specified")

    this.asInstanceOf[ClientBuilder[Req, Rep, Yes, Yes, Yes]]
  }

  /**
   * Construct a Service, with runtime checks for builder
   * completeness.
   */
  def unsafeBuild(): Service[Req, Rep] =
    validated.build()

  /**
   * Construct a ServiceFactory, with runtime checks for builder
   * completeness.
   */
  def unsafeBuildFactory(): ServiceFactory[Req, Rep] =
    validated.buildFactory()

  private def retryFilter(timer: com.twitter.util.Timer) =
    params[Retries] match {
      case Retries(policy) if params.contains[Retries] =>
        val Label(label) = params[Label]
        val stats = new StatsFilter[Req, Rep](statsReceiver.scope("tries"))
        val retries = new RetryingFilter[Req, Rep](policy, timer, statsReceiver)
        stats andThen retries
      case _ => identityFilter
    }

  private def globalTimeoutFilter(timer: com.twitter.util.Timer) = {
    val GlobalTimeout(timeout) = params[GlobalTimeout]
    if (timeout < Duration.Top) {
      val exception = new GlobalRequestTimeoutException(timeout)
      new TimeoutFilter[Req, Rep](timeout, exception, timer)
    } else {
      identityFilter
    }
  }

  private val identityFilter = Filter.identity[Req, Rep]
}
