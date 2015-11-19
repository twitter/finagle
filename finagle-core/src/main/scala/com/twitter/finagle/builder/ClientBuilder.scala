package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter.Credentials
import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient}
import com.twitter.finagle.client.{StackBasedClient, Transporter}
import com.twitter.finagle.factory.{BindingFactory, TimeoutFactory}
import com.twitter.finagle.filter.ExceptionSourceFilter
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.service._
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{NullTracer, TraceInitializerFilter}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.util
import com.twitter.util.{Duration, Future, NullMonitor, Time, Try}
import java.net.{InetSocketAddress, SocketAddress}
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

  /**
   * Returns a [[com.twitter.finagle.client.StackClient]] which is equivalent to a
   * `ClientBuilder` configured with the same codec; that is, given
   * {{{
   *   val cb = ClientBuilder()
   *     .dest(dest)
   *     .name(name)
   *     .codec(codec)
   *
   *   val sc = ClientBuilder.stackClientOfCodec(codec)
   * }}}
   * then the following are equivalent
   * {{{
   *   cb.build()
   *   sc.newService(dest, name)
   * }}}
   * and the following are also equivalent
   * {{{
   *   cb.buildFactory()
   *   sc.newClient(dest, name)
   * }}}
   */
  def stackClientOfCodec[Req, Rep](
    codecFactory: CodecFactory[Req, Rep]#Client
  ): StackClient[Req, Rep] =
    ClientBuilderClient(CodecClient[Req, Rep](codecFactory))
}

object ClientConfig {
  sealed trait Yes
  type FullySpecified[Req, Rep] = ClientConfig[Req, Rep, Yes, Yes, Yes]
  val DefaultName = "client"

  private case class NilClient[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]] = StackClient.newStack[Req, Rep],
    params: Stack.Params = DefaultParams
  ) extends StackBasedClient[Req, Rep] {

    def withParams(ps: Stack.Params) = copy(params = ps)
    def transformed(t: Stack.Transformer) = copy(stack = t(stack))

    def newService(dest: Name, label: String): Service[Req, Rep] =
      newClient(dest, label).toService

    def newClient(dest: Name, label: String): ServiceFactory[Req, Rep] =
      ServiceFactory(() => Future.value(Service.mk[Req, Rep](_ => Future.exception(
        new Exception("unimplemented")))))
  }

  def nilClient[Req, Rep]: StackBasedClient[Req, Rep] = NilClient[Req, Rep]()

  // params specific to ClientBuilder
  private[builder] case class DestName(name: Name) {
    def mk(): (DestName, Stack.Param[DestName]) =
      (this, DestName.param)
  }
  private[builder] object DestName {
    implicit val param = Stack.Param(DestName(Name.empty))
  }

  private[builder] case class GlobalTimeout(timeout: Duration) {
    def mk(): (GlobalTimeout, Stack.Param[GlobalTimeout]) =
      (this, GlobalTimeout.param)
  }
  private[builder] object GlobalTimeout {
    implicit val param = Stack.Param(GlobalTimeout(Duration.Top))
  }

  private[builder] case class Daemonize(onOrOff: Boolean) {
    def mk(): (Daemonize, Stack.Param[Daemonize]) =
      (this, Daemonize.param)
  }
  private[builder] object Daemonize {
    implicit val param = Stack.Param(Daemonize(true))
  }

  private[builder] case class MonitorFactory(mFactory: String => util.Monitor) {
    def mk(): (MonitorFactory, Stack.Param[MonitorFactory]) =
      (this, MonitorFactory.param)
  }
  private[builder] object MonitorFactory {
    implicit val param = Stack.Param(MonitorFactory(_ => NullMonitor))
  }

  // historical defaults for ClientBuilder
  private[builder] val DefaultParams = Stack.Params.empty +
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

/**
 * A builder of Finagle [[com.twitter.finagle.Client Clients]].
 *
 * Please see the
 * [[http://twitter.github.io/finagle/guide/FAQ.html#configuring-finagle6 Finagle user guide]]
 * for information on a newer set of client-construction APIs introduced in Finagle v6.
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
 *
 * =Defaults=
 *
 * The following defaults are applied to clients constructed via ClientBuilder,
 * unless overridden with the corresponding method. These defaults were chosen
 * carefully so as to work well for most use cases.
 *
 * Commonly-configured options:
 *
 *  - `connectTimeout`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `tcpConnectTimeout`: 1 second
 *  - `requestTimeout`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `timeout`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `hostConnectionLimit`: `Int.MaxValue`
 *  - `hostConnectionCoresize`: 0
 *  - `hostConnectionIdleTime`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `hostConnectionMaxWaiters`: `Int.MaxValue`
 *  - `failFast`: true
 *  - `failureAccrualParams`, `failureAccrualFactory`:
 *    `numFailures` = 5, `markDeadFor` = 5 seconds
 *
 * Advanced options:
 *
 * ''Before changing any of these, make sure that you know exactly how they will
 * affect your application -- these options are typically only changed by expert
 * users.''
 *
 *  - `keepAlive`: Unspecified, in which case the
 *    [[http://docs.oracle.com/javase/7/docs/api/java/net/StandardSocketOptions.html?is-external=true#SO_KEEPALIVE Java default]]
 *    of `false` is used
 *  - `readerIdleTimeout`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `writerIdleTimeout`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `hostConnectionMaxIdleTime`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `hostConnectionMaxLifeTime`: [[com.twitter.util.Duration.Top Duration.Top]]
 *  - `sendBufferSize`, `recvBufferSize`: OS-defined default value
 */
class ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit] private[finagle](
  client: StackBasedClient[Req, Rep]
) {
  import ClientConfig._
  import com.twitter.finagle.param._

  // Convenient aliases.
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
  type ThisConfig           = ClientConfig[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]
  type This                 = ClientBuilder[Req, Rep, HasCluster, HasCodec, HasHostConnectionLimit]

  private[builder] def this() = this(ClientConfig.nilClient)

  override def toString() = "ClientBuilder(%s)".format(params)

  private def copy[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    client: StackBasedClient[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster1, HasCodec1, HasHostConnectionLimit1] =
    new ClientBuilder(client)

  private def configured[P: Stack.Param, HasCluster1, HasCodec1, HasHostConnectionLimit1](
    param: P
  ): ClientBuilder[Req, Rep, HasCluster1, HasCodec1, HasHostConnectionLimit1] =
    copy(client.configured(param))

  // Used in deprecated KetamaClientBuilder, remove when we drop it in
  // favor of the finagle.Memcached protocol object.
  private[finagle] def underlying: StackBasedClient[Req, Rep] = client

  def params: Stack.Params = client.params

  /**
   * Specify the set of hosts to connect this client to.  Requests
   * will be load balanced across these.  This is a shorthand form for
   * specifying a cluster.
   *
   * One of the {{hosts}} variations or direct specification of the
   * cluster (via {{cluster}}) is required.
   *
   * @param hostnamePortCombinations comma-separated "host:port"
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
   * The base [[com.twitter.finagle.Dtab]] used to interpret logical
   * destinations for this client. (This is given as a function to
   * permit late initialization of [[com.twitter.finagle.Dtab.base]].)
   */
  def baseDtab(baseDtab: () => Dtab): This =
    configured(BindingFactory.BaseDtab(baseDtab))

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

  /**
   * Specify a load balancer.  The load balancer implements
   * a strategy for choosing one from a set of hosts to service a request
   */
  def loadBalancer(loadBalancer: LoadBalancerFactory): This =
    configured(LoadBalancerFactory.Param(loadBalancer))

  /**
   * Specify the codec. The codec implements the network protocol
   * used by the client, and consequently determines the `Req` and `Rep`
   * type variables. One of the codec variations is required.
   */
  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(Function.const(codec)(_))
      .configured(ProtocolLibrary(codec.protocolLibraryName))

  /**
   * A variation of `codec` that supports codec factories.  This is
   * used by codecs that need dynamic construction, but should be
   * transparent to the user.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(codecFactory.client)
      .configured(ProtocolLibrary(codecFactory.protocolLibraryName))

  /**
   * A variation of codec for codecs that support only client-codecs.
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Client
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    copy(CodecClient[Req1, Rep1](codecFactory).withParams(params))

  /**
   * Overrides the stack and [[com.twitter.finagle.Client]] that will be used
   * by this builder.
   *
   * @param client A `StackBasedClient` representation of a
   * [[com.twitter.finagle.Client]]. `client` is materialized with the state of
   * configuration when `build` is called. There is no guarantee that all
   * builder parameters will be used by the resultant `Client`; it is up to the
   * discretion of `client` itself and the protocol implementation. For example,
   * the Mux protocol has no use for most connection pool parameters (e.g.
   * `hostConnectionLimit`). Thus when configuring
   * [[com.twitter.finagle.ThriftMux]] clients (via [[stack(ThriftMux.client)]]),
   * such connection pool parameters will not be applied.
   */
  def stack[Req1, Rep1](
    client: StackBasedClient[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, Yes] = {
    copy(client.withParams(client.params ++ params))
  }

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
   * cannot create more connections due to `hostConnectionLimit` and
   * there are more than `hostConnectionLimit` requests outstanding)
   * as well as physical connection time.  Futures returned from
   * `factory()` will always be satisfied within this timeout.
   *
   * This timeout is also used for name resolution, separately from
   * queueing and physical connection time, so in the worst case the
   * time to acquire a service may be double the given duration before
   * timing out.
   */
  def connectTimeout(duration: Duration): This =
    configured(TimeoutFactory.Param(duration))

  /**
   * Total request timeout.  This timeout is applied from the issuance
   * of a request (through `service(request)`) until the
   * satisfaction of that reply future.  No request will take longer
   * than this.
   *
   * Applicable only to service-builds (`build()`)
   */
  def timeout(duration: Duration): This =
    configured(GlobalTimeout(duration))

  /**
   * Apply TCP keepAlive (`SO_KEEPALIVE` socket option).
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
   * Report stats to the given `StatsReceiver`.  This will report
   * verbose global statistics and counters, that in turn may be
   * exported to monitoring applications.
   *
   * @note Per hosts statistics will '''NOT''' be exported to this receiver
   *
   * @see [[ClientBuilder.reportHostStats]]
   */
  def reportTo(receiver: StatsReceiver): This =
    configured(Stats(receiver))

  /**
   * Report per host stats to the given `StatsReceiver`.
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
   * @note This will be integrated into the mainline pool, at
   * which time the experimental option will go away.
   */
  def expHostConnectionBufferSize(size: Int): This =
    configured(params[DefaultPool.Param].copy(bufferSize = size))

  /**
   * Retry (some) failed requests up to `value - 1` times.
   *
   * Retries are only done if the request failed with something
   * known to be safe to retry. This includes [[WriteException WriteExceptions]]
   * and [[Failure]]s that are marked [[Failure.Restartable restartable]].
   *
   * The configured policy has jittered backoffs between retries.
   *
   * @param value the maximum number of attempts (including retries) that
   *              can be made.
   *               - A value of `1` means one attempt and no retries
   *              on failure.
   *               - A value of `2` means one attempt and then a
   *              single retry if the failure is known to be safe to retry.
   *
   * @note The failures seen in the client will '''not include'''
   *       application level failures. This is particularly important for
   *       codecs that include exceptions, such as `Thrift`.
   *
   *       This is only applicable to service-builds (`build()`).
   *
   * @see [[com.twitter.finagle.service.RetryPolicy.tries]]
   *
   * @see [[retryBudget]] for governing how many failed requests are
   * eligible for retries.
   */
  def retries(value: Int): This =
    retryPolicy(RetryPolicy.tries(value))

  /**
   * Retry failed requests according to the given [[RetryPolicy]].
   *
   * @note The failures seen in the client will '''not include'''
   *       application level failures. This is particularly important for
   *       codecs that include exceptions, such as `Thrift`.
   *
   *       This is only applicable to service-builds (`build()`).
   *
   * @see [[retryBudget]] for governing how many failed requests are
   * eligible for retries.
   */
  def retryPolicy(value: RetryPolicy[Try[Nothing]]): This =
    configured(Retries.Policy(value))

  /**
   * The [[RetryBudget budget]] is shared across requests and governs
   * the number of retries that can be made.
   *
   * Helps prevent clients from overwhelming the downstream service.
   *
   * @see [[retryPolicy]] for per-request rules on which failures are
   * eligible for retries.
   */
  def retryBudget(budget: RetryBudget): This =
    configured(Retries.Budget(budget))

  /**
   * The [[RetryBudget budget]] is shared across requests and governs
   * the number of retries that can be made. When used for requeues,
   * includes a stream of delays used to delay each retry.
   *
   * Helps prevent clients from overwhelming the downstream service.
   *
   * @see [[retryPolicy]] for per-request rules on which failures are
   * eligible for retries.
   */
  def retryBudget(budget: RetryBudget, backoffSchedule: Stream[Duration]): This =
    configured(Retries.Budget(budget, backoffSchedule))

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
   * responsible for calling `releaseExternalResources()`.
   */
  def channelFactory(cf: ChannelFactory): This =
    configured(Netty3Transporter.ChannelFactory(cf))

  /**
   * Encrypt the connection with SSL.  Hostname verification will be
   * provided against the given hostname.
   */
  def tls(hostname: String): This = {
    configured((Transport.TLSClientEngine(Some({
      case inet: InetSocketAddress => Ssl.client(hostname, inet.getPort)
      case _ => Ssl.client()
    }))))
      .configured(Transporter.TLSHostname(Some(hostname)))
  }

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * No SSL Hostname Validation is performed
   */
  def tls(sslContext: SSLContext): This =
    configured((Transport.TLSClientEngine(Some({
      case inet: InetSocketAddress => Ssl.client(sslContext, inet.getHostName, inet.getPort)
      case _ => Ssl.client(sslContext)
    }))))

  /**
   * Encrypt the connection with SSL.  The Engine to use can be passed into the client.
   * This allows the user to use client certificates
   * SSL Hostname Validation is performed, on the passed in hostname
   */
  def tls(sslContext: SSLContext, hostname: Option[String]): This =
    configured((Transport.TLSClientEngine(Some({
      case inet: InetSocketAddress => Ssl.client(sslContext, hostname.getOrElse(inet.getHostName), inet.getPort)
      case _ => Ssl.client(sslContext)
    }))))
      .configured(Transporter.TLSHostname(hostname))

  /**
   * Do not perform TLS validation. Probably dangerous.
   */
  def tlsWithoutValidation(): This =
    configured(Transport.TLSClientEngine(Some({
      case inet: InetSocketAddress => Ssl.clientWithoutCertificateValidation(inet.getHostName, inet.getPort)
      case _ => Ssl.clientWithoutCertificateValidation()
    })))

  /**
   * Make connections via the given HTTP proxy.
   * If this is defined concurrently with socksProxy, the order in which they are applied is undefined.
   */
  def httpProxy(httpProxy: SocketAddress): This =
    configured(params[Transporter.HttpProxy].copy(sa = Some(httpProxy)))

  /**
   * For the http proxy use these [[Credentials]] for authentication.
   */
  def httpProxyUsernameAndPassword(credentials: Credentials): This =
    configured(params[Transporter.HttpProxy].copy(credentials = Some(credentials)))

  @deprecated("Use socksProxy(socksProxy: Option[SocketAddress])", "2014-12-02")
  def socksProxy(socksProxy: SocketAddress): This =
    configured(params[Transporter.SocksProxy].copy(sa = Some(socksProxy)))

  /**
   * Make connections via the given SOCKS proxy.
   * If this is defined concurrently with httpProxy, the order in which they are applied is undefined.
   */
  def socksProxy(socksProxy: Option[SocketAddress]): This =
    configured(params[Transporter.SocksProxy].copy(sa = socksProxy))

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
   * Use the given parameters for failure accrual.  The first parameter
   * is the number of *successive* failures that are required to mark
   * a host failed.  The second parameter specifies how long the host
   * is dead for, once marked.
   *
   * To completely disable [[FailureAccrualFactory]] use `noFailureAccrual`.
   */
  def failureAccrualParams(pair: (Int, Duration)): This = {
    val (numFailures, markDeadFor) = pair
    configured(FailureAccrualFactory.Param(numFailures, () => markDeadFor))
  }

  /**
   * Disables [[FailureAccrualFactory]].
   *
   * To replace the [[FailureAccrualFactory]] use `failureAccrualFactory`.
   */
  def noFailureAccrual: This =
    configured(FailureAccrualFactory.Disabled)

  /**
   * Completely replaces the [[FailureAccrualFactory]] from the underlying stack
   * with the [[ServiceFactoryWrapper]] returned from the given function `factory`.
   *
   * To completely disable [[FailureAccrualFactory]] use `noFailureAccrual`.
   */
  def failureAccrualFactory(factory: util.Timer => ServiceFactoryWrapper): This =
    configured(FailureAccrualFactory.Replaced(factory))

  @deprecated(
    "No longer experimental: Use failFast()." +
    "The new default value is true, so replace .expFailFast(true) with nothing at all",
    "5.3.10")
  def expFailFast(enabled: Boolean): This =
    failFast(enabled)

  /**
   * Marks a host dead on connection failure. The host remains dead
   * until we successfully connect. Intermediate connection attempts
   * *are* respected, but host availability is turned off during the
   * reconnection period.
   */
  def failFast(enabled: Boolean): This =
    configured(FailFast(enabled))

  /**
   * When true, the client is daemonized. As with java threads, a
   * process can exit only when all remaining clients are daemonized.
   * False by default.
   */
  def daemon(daemonize: Boolean): This =
    configured(Daemonize(daemonize))

  /**
   * Provide an alternative to putting all request exceptions under
   * a "failures" stat.  Typical implementations may report any
   * cancellations or validation errors separately so success rate
   * considers only valid non cancelled requests.
   *
   * @param exceptionStatsHandler function to record failure details.
   */
  def exceptionCategorizer(exceptionStatsHandler: stats.ExceptionStatsHandler): This =
    configured(ExceptionStatsHandler(exceptionStatsHandler))

  /**
   * Configures the traffic class.
   *
   * @see [[Transporter.TrafficClass]]
   */
  def trafficClass(value: Option[Int]): This =
    configured(Transporter.TrafficClass(value))

  /*** BUILD ***/

  // This is only used for client alterations outside of the stack.
  // a more ideal usage would be to retrieve the stats param inside your specific module
  // instead of using this statsReceiver as it keeps the params closer to where they're used
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
    val Label(label) = params[Label]
    val DestName(dest) = params[DestName]
    ClientBuilderClient.newClient(client, dest, label)
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
    val Label(label) = params[Label]
    val DestName(dest) = params[DestName]
    ClientBuilderClient.newService(client, dest, label)
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
}

/**
 * A [[com.twitter.finagle.client.StackClient]] which adds the
 * filters historically included in `ClientBuilder` clients.
 */
private case class ClientBuilderClient[Req, Rep](
  client: StackClient[Req, Rep]
) extends StackClient[Req, Rep] {

  def params = client.params
  def withParams(ps: Stack.Params) = copy(client.withParams(ps))
  def stack = client.stack
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]) = copy(client.withStack(stack))

  def newClient(dest: Name, label: String) =
    ClientBuilderClient.newClient(client, dest, label)

  def newService(dest: Name, label: String) =
    ClientBuilderClient.newService(client, dest, label)
}

private object ClientBuilderClient {
  import ClientConfig._
  import com.twitter.finagle.param._

  private class StatsFilterModule[Req, Rep]
      extends Stack.Module2[Stats, ExceptionStatsHandler, ServiceFactory[Req, Rep]] {
    override val role = new Stack.Role("ClientBuilder StatsFilter")
    override val description =
      "Record request stats scoped to 'tries', measured after any retries have occurred"

    override def make(
      statsP: Stats,
      exceptionStatsHandlerP: ExceptionStatsHandler,
      next: ServiceFactory[Req, Rep]
    ) = {
      val Stats(statsReceiver) = statsP
      val ExceptionStatsHandler(categorizer) = exceptionStatsHandlerP

      val stats = new StatsFilter[Req, Rep](statsReceiver.scope("tries"), categorizer)
      stats andThen next
    }
  }

  private class GlobalTimeoutModule[Req, Rep]
      extends Stack.Module2[GlobalTimeout, Timer, ServiceFactory[Req, Rep]] {
    override val role = new Stack.Role("ClientBuilder GlobalTimeoutFilter")
    override val description = "Application-configured global timeout"

    override def make(
      globalTimeoutP: GlobalTimeout,
      timerP: Timer,
      next: ServiceFactory[Req, Rep]
    ) = {
      val GlobalTimeout(timeout) = globalTimeoutP
      val Timer(timer) = timerP

      if (timeout == Duration.Top) next
      else {
        val exception = new GlobalRequestTimeoutException(timeout)
        val globalTimeout = new TimeoutFilter[Req, Rep](timeout, exception, timer)
        globalTimeout andThen next
      }
    }
  }

  private class ExceptionSourceFilterModule[Req, Rep]
      extends Stack.Module1[Label, ServiceFactory[Req, Rep]] {
    override val role = new Stack.Role("ClientBuilder ExceptionSourceFilter")
    override val description = "Exception source filter"

    override def make(
      labelP: Label,
      next: ServiceFactory[Req, Rep]
    ) = {
      val Label(label) = labelP

      val exceptionSource = new ExceptionSourceFilter[Req, Rep](label)
      exceptionSource andThen next
    }
  }

  def newClient[Req, Rep](
    client: StackBasedClient[Req, Rep],
    dest: Name,
    label: String
  ): ServiceFactory[Req, Rep] = {
    val params = client.params
    val Daemonize(daemon) = params[Daemonize]
    val Logger(logger) = params[Logger]
    val MonitorFactory(mFactory) = params[MonitorFactory]

    val clientParams = params + Monitor(mFactory(label))

    val factory = client.withParams(clientParams).newClient(dest, label)

    val exitGuard = if (!daemon) Some(ExitGuard.guard(s"client for '$label'")) else None
    new ServiceFactoryProxy[Req, Rep](factory) {
      private[this] val closed = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!closed.compareAndSet(false, true)) {
          logger.log(Level.WARNING, "Close on ServiceFactory called multiple times!",
            new Exception/*stack trace please*/)
          return Future.exception(new IllegalStateException)
        }

        super.close(deadline) ensure {
          exitGuard.foreach(_.unguard())
        }
      }
    }
  }

  def newService[Req, Rep](
    client0: StackBasedClient[Req, Rep],
    dest: Name,
    label: String
  ): Service[Req, Rep] = {
    val client =
      client0
        .transformed(new Stack.Transformer {
          def apply[Req, Rep](stack: Stack[ServiceFactory[Req, Rep]]) =
            stack
              .insertBefore(Retries.Role, new StatsFilterModule[Req, Rep])
              .replace(Retries.Role, Retries.moduleWithRetryPolicy[Req, Rep])
              .prepend(new GlobalTimeoutModule[Req, Rep])
              .prepend(new ExceptionSourceFilterModule[Req, Rep])
        })
        .configured(FactoryToService.Enabled(true))

    val factory = newClient(client, dest, label)
    val service: Service[Req, Rep] = new FactoryToService[Req, Rep](factory)

    new ServiceProxy[Req, Rep](service) {
      private[this] val released = new AtomicBoolean(false)
      override def close(deadline: Time): Future[Unit] = {
        if (!released.compareAndSet(false, true)) {
          val Logger(logger) = client.params[Logger]
          logger.log(java.util.logging.Level.WARNING, "Release on Service called multiple times!",
            new Exception/*stack trace please*/)
          return Future.exception(new IllegalStateException)
        }
        super.close(deadline)
      }
    }
  }
}

/**
 * A [[com.twitter.finagle.client.StackClient]] based on a
 * [[com.twitter.finagle.Codec]].
 */
private case class CodecClient[Req, Rep](
  codecFactory: CodecFactory[Req, Rep]#Client,
  stack: Stack[ServiceFactory[Req, Rep]] = StackClient.newStack[Req, Rep],
  params: Stack.Params = ClientConfig.DefaultParams
) extends StackClient[Req, Rep] {
  import com.twitter.finagle.param._

  def withParams(ps: Stack.Params) = copy(params = ps)
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]) = copy(stack = stack)

  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep] = {
    val codec = codecFactory(ClientCodecConfig(label))

    val prepConn = new Stack.Module1[Stats, ServiceFactory[Req, Rep]] {
      val role = StackClient.Role.prepConn
      val description = "Connection preparation phase as defined by a Codec"
      def make(_stats: Stats, next: ServiceFactory[Req, Rep]) = {
        val Stats(stats) = _stats
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

    val clientStack = {
      val stack0 = stack
        .replace(StackClient.Role.prepConn, prepConn)
        .replace(StackClient.Role.prepFactory, (next: ServiceFactory[Req, Rep]) =>
        codec.prepareServiceFactory(next))
        .replace(TraceInitializerFilter.role, codec.newTraceInitializer)

      // disable failFast if the codec requests it or it is
      // disabled via the ClientBuilder parameter.
      val FailFast(failFast) = params[FailFast]
      if (!codec.failFastOk || !failFast) stack0.remove(FailFastFactory.role) else stack0
    }

    case class Client(
      stack: Stack[ServiceFactory[Req, Rep]] = clientStack,
      params: Stack.Params = params
    ) extends StdStackClient[Req, Rep, Client] {
      protected def copy1(
        stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
        params: Stack.Params = this.params): Client = copy(stack, params)

      protected type In = Any
      protected type Out = Any

      protected def newTransporter(): Transporter[Any, Any] = {
        val Stats(stats) = params[Stats]
        val newTransport = (ch: Channel) => codec.newClientTransport(ch, stats)
        Netty3Transporter[Any, Any](codec.pipelineFactory,
          params + Netty3Transporter.TransportFactory(newTransport))
      }

      protected def newDispatcher(transport: Transport[In, Out]) =
        codec.newClientDispatcher(transport, params)
    }

    val proto = params[ProtocolLibrary]

    // don't override a configured protocol value
    val clientParams =
      if (proto != ProtocolLibrary.param.default) params
      else params + ProtocolLibrary(codec.protocolLibraryName)

    Client(
      stack = clientStack,
      params = clientParams
    ).newClient(dest, label)
  }

  // not called
  def newService(dest: Name, label: String): Service[Req, Rep] = ???
}
