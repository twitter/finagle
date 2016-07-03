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
import scala.annotation.{implicitNotFound, varargs}

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
 * [[http://twitter.github.io/finagle/guide/Configuration.html Finagle user guide]]
 * for information on the preferred `with`-style client-construction APIs.
 *
 * {{{
 * val client = ClientBuilder()
 *   .codec(Http)
 *   .hosts("localhost:10000,localhost:10001,localhost:10003")
 *   .hostConnectionLimit(1)
 *   .tcpConnectTimeout(1.second)        // max time to spend establishing a TCP connection.
 *   .retries(2)                         // (1) per-request retries
 *   .reportTo(DefaultStatsReceiver)     // export host-level load data to the loaded-StatsReceiver
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
 *      .reportTo(DefaultStatsReceiver)
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
 *
 * @see The [[http://twitter.github.io/finagle/guide/Configuration.html user guide]]
 *      for information on the preferred `with`-style APIs insead.
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
   * One of the `hosts` variations or direct specification of the
   * cluster (via `cluster`) is required.
   *
   * To migrate to the Stack-based APIs, pass the hostname and port
   * pairs into `com.twitter.finagle.Client.newService(String)`. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.newService("hostnameA:portA,hostnameB:portB")
   * }}}
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
   * A variant of `hosts` that takes a sequence of
   * [[java.net.InetSocketAddress]] instead.
   *
   * To migrate to the Stack-based APIs,
   * use `com.twitter.finagle.Client.newService(Name, String)`.
   * For the label String, use the scope you want for your [[StatsReceiver]].
   * For example:
   * {{{
   * import com.twitter.finagle.{Address, Http, Name}
   *
   * val addresses: Seq[Address] = sockaddrs.map(Address(_))
   * val name: Name = Name.bound(addresses: _*)
   * Http.client.newService(name, "the_client_name")
   * }}}
   */
  def hosts(
    sockaddrs: Seq[InetSocketAddress]
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    addrs(sockaddrs.map(Address(_)): _*)

  /**
   * A convenience method for specifying a one-host
   * [[java.net.SocketAddress]] client.
   *
   * To migrate to the Stack-based APIs,
   * use `com.twitter.finagle.Client.newService(Name, String)`.
   * For the label String, use the scope you want for your [[StatsReceiver]].
   * For example:
   * {{{
   * import com.twitter.finagle.{Address, Http, Name}
   *
   * val name: Name = Name.bound(Address(address))
   * Http.client.newService(name, "the_client_name")
   * }}}
   */
  def hosts(
    address: InetSocketAddress
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    hosts(Seq(address))

  /**
   * A convenience method for specifying a client with one or more
   * [[com.twitter.finagle.Address]]s.
   *
   * To migrate to the Stack-based APIs,
   * use `com.twitter.finagle.Client.newService(Name, String)`.
   * For the label String, use the scope you want for your [[StatsReceiver]].
   * For example:
   * {{{
   * import com.twitter.finagle.{Http, Name}
   *
   * val name: Name = Name.bound(addrs: _*)
   * Http.client.newService(name, "the_client_name")
   * }}}
   */
  @varargs
  def addrs(
    addrs: Address*
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    dest(Name.bound(addrs:_*))

  /**
   * The logical destination of requests dispatched through this
   * client, as evaluated by a resolver. If the name evaluates a
   * label, this replaces the builder's current name.
   *
   * To migrating to the Stack-based APIs, you pass the destination
   * to `newClient` or `newService`. If the `addr` is labeled,
   * additionally, use `CommonParams.withLabel`
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client
   *   .withLabel("client_name")
   *   .newService(name)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use this in the call to `newClient`
   * or `newService`. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.newService(name)
   * }}}
   */
  def dest(
    name: Name
  ): ClientBuilder[Req, Rep, Yes, HasCodec, HasHostConnectionLimit] =
    configured(DestName(name))

  /**
   * The base [[com.twitter.finagle.Dtab]] used to interpret logical
   * destinations for this client. (This is given as a function to
   * permit late initialization of [[com.twitter.finagle.Dtab.base]].)
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.factory.BindingFactory
   *
   * Http.client.configured(BindingFactory.BaseDtab(baseDtab))
   * }}}
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
   * a strategy for choosing one host from a set to service a request.
   *
   * To migrate to the Stack-based APIs, use `withLoadBalancer(LoadBalancerFactory)`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withLoadBalancer(loadBalancer)
   * }}}
   */
  def loadBalancer(loadBalancer: LoadBalancerFactory): This =
    configured(LoadBalancerFactory.Param(loadBalancer))

  /**
   * Specify the codec. The codec implements the network protocol
   * used by the client, and consequently determines the `Req` and `Rep`
   * type variables. One of the codec variations is required.
   *
   * To migrate to the Stack-based APIs, use `ClientBuilder.stack(Protocol.client)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder().stack(Http.client)
   * }}}
   */
  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(Function.const(codec)(_))

  /**
   * A variation of `codec` that supports codec factories.  This is
   * used by codecs that need dynamic construction, but should be
   * transparent to the user.
   *
   * To migrate to the Stack-based APIs, use `ClientBuilder.stack(Protocol.client)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder().stack(Http.client)
   * }}}
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] =
    this.codec(codecFactory.client)

  /**
   * A variation of codec for codecs that support only client-codecs.
   *
   * To migrate to the Stack-based APIs, use `ClientBuilder.stack(Protocol.client)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder().stack(Http.client)
   * }}}
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Client
  ): ClientBuilder[Req1, Rep1, HasCluster, Yes, HasHostConnectionLimit] = {
    // in order to know the protocol library name, we need to produce
    // a throw-away codec. given that the codec API is on its way out
    // in favor of Stack, this is a reasonable compromise.
    val codec = codecFactory(ClientCodecConfig("ClientBuilder protocolLibraryName"))
    copy(CodecClient[Req1, Rep1](codecFactory).withParams(params))
      .configured(ProtocolLibrary(codec.protocolLibraryName))
  }

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
   *
   * To migrate to the Stack-based APIs, use `ClientTransportParams.connectTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.connectTimeout(duration)
   * }}}
   */
  def tcpConnectTimeout(duration: Duration): This =
    configured(Transporter.ConnectTimeout(duration))

  /**
   * The request timeout is the time given to a *single* request (if
   * there are retries, they each get a fresh request timeout).  The
   * timeout is applied only after a connection has been acquired.
   * That is: it is applied to the interval between the dispatch of
   * the request and the receipt of the response.
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withRequestTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withRequestTimeout(duration)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `SessionParams.acquisitionTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSession.acquisitionTimeout(duration)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use this method in conjunction with
   * `ClientBuilder.stack`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder()
   *   .stack(Http.client)
   *   .timeout(duration)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `TransportParams.readTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.readTimeout(duration)
   * }}}
   */
  def readerIdleTimeout(duration: Duration): This =
    configured(params[Transport.Liveness].copy(readTimeout = duration))

  /**
   * The maximum time a connection may not have sent any data.
   *
   * To migrate to the Stack-based APIs, use `TransportParams.writeTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.writeTimeout(duration)
   * }}}
   */
  def writerIdleTimeout(duration: Duration): This =
    configured(params[Transport.Liveness].copy(writeTimeout = duration))

  /**
   * Report stats to the given `StatsReceiver`.  This will report
   * verbose global statistics and counters, that in turn may be
   * exported to monitoring applications.
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withStatsReceiver`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withStatsReceiver(receiver)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.loadbalancer.LoadBalancerFactory
   *
   * Http.client.configured(LoadBalancerFactory.HostStats(receiver))
   * }}}
   */
  def reportHostStats(receiver: StatsReceiver): This =
    configured(LoadBalancerFactory.HostStats(receiver))

  /**
   * Give a meaningful name to the client. Required.
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withLabel`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withLabel("my_cool_client")
   * }}}
   */
  def name(value: String): This =
    configured(Label(value))

  /**
   * The maximum number of connections that are allowed per host.
   * Required.  Finagle guarantees to never have more active
   * connections than this limit.
   *
   * To migrate to the Stack-based APIs, use `SessionPoolingParams.maxSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSessionPool.maxSize(value)
   * }}}
   *
   * @note not all protocol implementations support this style of connection
   *       pooling, such as `com.twitter.finagle.ThriftMux` and
   *       `com.twitter.finagle.Memcached`.
   */
  def hostConnectionLimit(value: Int): ClientBuilder[Req, Rep, HasCluster, HasCodec, Yes] =
    configured(params[DefaultPool.Param].copy(high = value))

  /**
   * The core size of the connection pool: the pool is not shrinked below this limit.
   *
   * To migrate to the Stack-based APIs, use `SessionPoolingParams.minSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSessionPool.minSize(value)
   * }}}
   *
   * @note not all protocol implementations support this style of connection
   *       pooling, such as `com.twitter.finagle.ThriftMux` and
   *       `com.twitter.finagle.Memcached`.
   */
  def hostConnectionCoresize(value: Int): This =
    configured(params[DefaultPool.Param].copy(low = value))

  /**
   * The amount of time a connection is allowed to linger (when it
   * otherwise would have been closed by the pool) before being
   * closed.
   *
   * @note not all protocol implementations support this style of connection
   *       pooling, such as `com.twitter.finagle.ThriftMux` and
   *       `com.twitter.finagle.Memcached`.
   */
  def hostConnectionIdleTime(timeout: Duration): This =
    configured(params[DefaultPool.Param].copy(idleTime = timeout))

  /**
   * The maximum queue size for the connection pool.
   *
   * To migrate to the Stack-based APIs, use `SessionPoolingParams.maxWaiters`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSessionPool.maxWaiters(nWaiters)
   * }}}
   *
   * @note not all protocol implementations support this style of connection
   *       pooling, such as `com.twitter.finagle.ThriftMux` and
   *       `com.twitter.finagle.Memcached`.
   */
  def hostConnectionMaxWaiters(nWaiters: Int): This =
    configured(params[DefaultPool.Param].copy(maxWaiters = nWaiters))

  /**
   * The maximum time a connection is allowed to linger unused.
   *
   * To migrate to the Stack-based APIs, use `SessionParams.maxIdleTime`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSession.maxIdleTime(timeout)
   * }}}
   */
  def hostConnectionMaxIdleTime(timeout: Duration): This =
    configured(params[ExpiringService.Param].copy(idleTime = timeout))

  /**
   * The maximum time a connection is allowed to exist, regardless of occupancy.
   *
   * To migrate to the Stack-based APIs, use `SessionParams.maxLifeTime`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSession.maxLifeTime(timeout)
   * }}}
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
   *
   * @note not all protocol implementations support this style of connection
   *       pooling, such as `com.twitter.finagle.ThriftMux` and
   *       `com.twitter.finagle.Memcached`.
   */
  def expHostConnectionBufferSize(size: Int): This =
    configured(params[DefaultPool.Param].copy(bufferSize = size))

  /**
   * Configure a [[com.twitter.finagle.service.ResponseClassifier]]
   * which is used to determine the result of a request/response.
   *
   * This allows developers to give Finagle the additional application-specific
   * knowledge necessary in order to properly classify them. Without this,
   * Finagle cannot make judgements about application level failures as it only
   * has a narrow understanding of failures (for example: transport level, timeouts,
   * and nacks).
   *
   * As an example take an HTTP client that receives a response with a 500 status
   * code back from a server. To Finagle this is a successful request/response
   * based solely on the transport level. The application developer may want to
   * treat all 500 status codes as failures and can do so via a
   * [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * It is a [[PartialFunction]] and as such multiple classifiers can be composed
   * together via [[PartialFunction.orElse]].
   *
   * Response classification is independently configured on the client and server.
   * For server-side response classification using [[com.twitter.finagle.builder.ServerBuilder]],
   * see [[com.twitter.finagle.builder.ServerBuilder.responseClassifier]]
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withResponseClassifier`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withResponseClassifier(classifier)
   * }}}
   *
   * @see `com.twitter.finagle.http.service.HttpResponseClassifier` for some
   * HTTP classification tools.
   *
   * @note If unspecified, the default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]]
   * which is a total function fully covering the input domain.
   */
  def responseClassifier(classifier: com.twitter.finagle.service.ResponseClassifier): This =
    configured(param.ResponseClassifier(classifier))

  /**
   * The currently configured [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * @note If unspecified, the default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]].
   */
  def responseClassifier: com.twitter.finagle.service.ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier

  /**
   * Retry (some) failed requests up to `value - 1` times.
   *
   * Retries are only done if the request failed with something
   * known to be safe to retry. This includes [[WriteException WriteExceptions]]
   * and [[Failure]]s that are marked [[Failure.Restartable restartable]].
   *
   * The configured policy has jittered backoffs between retries.
   *
   * To migrate to the Stack-based APIs, use this method in conjunction with
   * `ClientBuilder.stack`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder()
   *   .stack(Http.client)
   *   .retries(value)
   * }}}
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
   * To migrate to the Stack-based APIs, use this method in conjunction with
   * `ClientBuilder.stack`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ClientBuilder()
   *   .stack(Http.client)
   *   .retryPolicy(value)
   * }}}
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
   * To migrate to the Stack-based APIs, use `ClientParams.withRetryBudget`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withRetryBudget(budget)
   * }}}
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
   * To migrate to the Stack-based APIs, use `ClientParams.withRetryBudget`
   * and `ClientParams.withRetryBackoff`
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client
   *   .withRetryBudget(budget)
   *   .withRetryBackoff(backoffSchedule)
   * }}}
   *
   * @see [[retryPolicy]] for per-request rules on which failures are
   * eligible for retries.
   */
  def retryBudget(budget: RetryBudget, backoffSchedule: Stream[Duration]): This =
    configured(Retries.Budget(budget, backoffSchedule))

  /**
   * Sets the TCP send buffer size.
   *
   * To migrate to the Stack-based APIs, use `TransportParams.sendBufferSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.sendBufferSize(value)
   * }}}
   */
  def sendBufferSize(value: Int): This =
    configured(params[Transport.BufferSizes].copy(send = Some(value)))

  /**
   * Sets the TCP recv buffer size.
   *
   * To migrate to the Stack-based APIs, use `TransportParams.receiveBufferSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.receiveBufferSize(value)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `ClientTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.tls(hostname)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `ClientTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.tls(sslContext)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `ClientTransportParams.tlsWithoutValidation`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.tlsWithoutValidation
   * }}}
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
    * Make connections via the given HTTP proxy by host name and port.
    * The host name is resolved every transport connection.
    * This API is experiment.
    * If this is defined concurrently with socksProxy, the order in which they are applied is undefined.
    */
  def expHttpProxy(hostName: String, port: Int): This =
    configured(params[Transporter.HttpProxy].copy(sa = Some(InetSocketAddress.createUnresolved(hostName, port))))

  /**
   * For the http proxy use these [[Credentials]] for authentication.
   */
  def httpProxyUsernameAndPassword(credentials: Credentials): This =
    configured(params[Transporter.HttpProxy].copy(credentials = Some(credentials)))

  /**
   * Make connections via the given SOCKS proxy.
   * If this is defined concurrently with httpProxy, the order in which they are applied is undefined.
   *
   * To migrate to the Stack-based APIs, use `ClientTransportParams.socksProxy`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTransport.socksProxy(socketAddress, socksProxy)
   * }}}
   */
  def socksProxy(socksProxy: Option[SocketAddress]): This =
    configured(params[Transporter.SocksProxy].copy(sa = socksProxy))

  /**
    * Make connections via the given HTTP proxy by host name and port.
    * The host name is resolved every transport connection.
    * This API is experiment.
    * If this is defined concurrently with httpProxy, the order in which they are applied is undefined.
    */
  def expSocksProxy(hostName: String, port: Int): This =
    configured(
      params[Transporter.SocksProxy].copy(sa = Some(InetSocketAddress.createUnresolved(hostName, port)))
    )

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
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withTracer`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withTracer(t)
   * }}}
   */
  def tracer(t: com.twitter.finagle.tracing.Tracer): This =
    configured(Tracer(t))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withMonitor`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.util.Monitor
   *
   * val monitor: Monitor = ???
   * Http.client.withMonitor(monitor)
   * }}}
   */
  def monitor(mFactory: String => com.twitter.util.Monitor): This =
    configured(MonitorFactory(mFactory))

  /**
   * Log very detailed debug information to the given logger.
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.param.Logger
   *
   * Http.client.configured(Logger(logger))
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `SessionQualificationParams.noFailureAccrual`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSessionQualifier.noFailureAccrual
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `SessionQualificationParams.noFailFast`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withSessionQualifier.noFailFast
   * }}}
   */
  def failFast(enabled: Boolean): This =
    configured(FailFast(enabled))

  /**
   * When true, the client is daemonized. As with java threads, a
   * process can exit only when all remaining clients are daemonized.
   * False by default.
   *
   * The default for the Stack-based APIs is for the client to
   * be daemonized.
   */
  def daemon(daemonize: Boolean): This =
    configured(Daemonize(daemonize))

  /**
   * Provide an alternative to putting all request exceptions under
   * a "failures" stat.  Typical implementations may report any
   * cancellations or validation errors separately so success rate
   * considers only valid non cancelled requests.
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withExceptionStatsHandler`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withExceptionStatsHandler(exceptionStatsHandler)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `Client.newClient`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.newClient(destination)
   * }}}
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
   *
   * To migrate to the Stack-based APIs, use `Client.newService`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.newService(destination)
   * }}}
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
          def apply[Request, Response](stack: Stack[ServiceFactory[Request, Response]]) =
            stack
              .insertBefore(Retries.Role, new StatsFilterModule[Request, Response])
              .replace(Retries.Role, Retries.moduleWithRetryPolicy[Request, Response])
              .prepend(new GlobalTimeoutModule[Request, Response])
              .prepend(new ExceptionSourceFilterModule[Request, Response])
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

    val prepConn = new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      override def parameters: Seq[Stack.Param[_]] = Nil
      override val role = StackClient.Role.prepConn
      override val description = "Connection preparation phase as defined by a Codec"
      def make(ps: Stack.Params, next: ServiceFactory[Req, Rep]) = {
        val Stats(stats) = ps[Stats]
        val underlying = codec.prepareConnFactory(next, ps)
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
