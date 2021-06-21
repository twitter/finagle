package com.twitter.finagle.builder

import com.twitter.util
import com.twitter.finagle._
import com.twitter.finagle.filter.{MaskCancelFilter, ServerAdmissionControl}
import com.twitter.finagle.server.{Listener, StackBasedServer, StackServer}
import com.twitter.finagle.service.{ExpiringService, PendingRequestFilter, TimeoutFilter}
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.stats.{
  RelativeNameMarkingStatsReceiver,
  RoleConfiguredStatsReceiver,
  Server,
  StatsReceiver
}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.util.{CloseAwaitably, Duration, Future, NullMonitor, Time}
import java.net.SocketAddress
import scala.annotation.implicitNotFound

/**
 * Factory for [[com.twitter.finagle.builder.ServerBuilder]] instances
 */
object ServerBuilder {

  type Complete[Req, Rep] =
    ServerBuilder[Req, Rep, ServerConfig.Yes, ServerConfig.Yes, ServerConfig.Yes]

  @deprecated("Use the stack server pattern instead", "2021-06-21")
  def apply(): ServerBuilder[Nothing, Nothing, Nothing, Nothing, Nothing] =
    new ServerBuilder()

  @deprecated("Use the stack server pattern instead", "2021-06-21")
  def get(): ServerBuilder[Nothing, Nothing, Nothing, Nothing, Nothing] =
    apply()

  /**
   * Provides a typesafe `build` for Java.
   */
  def safeBuild[Req, Rep](
    service: Service[Req, Rep],
    builder: Complete[Req, Rep]
  ): ListeningServer =
    builder.build(service)(ServerConfigEvidence.FullyConfigured)

  /**
   * Provides a typesafe `build` for Java.
   */
  def safeBuild[Req, Rep](
    serviceFactory: ServiceFactory[Req, Rep],
    builder: Complete[Req, Rep]
  ): ListeningServer =
    builder.build(serviceFactory)(ServerConfigEvidence.FullyConfigured)
}

object ServerConfig {
  sealed trait Yes
  type FullySpecified[Req, Rep] = ServerConfig[Req, Rep, Yes, Yes, Yes]

  private case class NilServer[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]] = StackServer.newStack[Req, Rep],
    params: Stack.Params = Stack.Params.empty)
      extends StackBasedServer[Req, Rep] {

    def withParams(ps: Stack.Params): StackBasedServer[Req, Rep] = copy(params = ps)
    def transformed(t: Stack.Transformer): StackBasedServer[Req, Rep] = copy(stack = t(stack))

    def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer = NullServer
  }

  def nilServer[Req, Rep]: StackBasedServer[Req, Rep] = NilServer[Req, Rep]()

  // params specific to ServerBuilder
  private[builder] case class BindTo(addr: SocketAddress) {
    def mk(): (BindTo, Stack.Param[BindTo]) =
      (this, BindTo.param)
  }
  private[builder] object BindTo {
    implicit val param = Stack.Param(BindTo(new SocketAddress {
      override val toString = "unknown"
    }))
  }

  private[builder] case class MonitorFactory(mFactory: (String, SocketAddress) => util.Monitor) {
    def mk(): (MonitorFactory, Stack.Param[MonitorFactory]) =
      (this, MonitorFactory.param)
  }
  private[builder] object MonitorFactory {
    implicit val param = Stack.Param(MonitorFactory((_, _) => NullMonitor))
  }

  private[builder] case class Daemonize(onOrOff: Boolean) {
    def mk(): (Daemonize, Stack.Param[Daemonize]) =
      (this, Daemonize.param)
  }
  private[builder] object Daemonize {
    implicit val param = Stack.Param(Daemonize(false))
  }
}

@implicitNotFound(
  "Builder is not fully configured: Codec: ${HasCodec}, BindTo: ${HasBindTo}, Name: ${HasName}"
)
trait ServerConfigEvidence[HasCodec, HasBindTo, HasName]

private[builder] object ServerConfigEvidence {
  implicit object FullyConfigured
      extends ServerConfigEvidence[ServerConfig.Yes, ServerConfig.Yes, ServerConfig.Yes]
}

/**
 * A configuration object that represents what shall be built.
 */
private[builder] final class ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName]

/**
 * A handy Builder for constructing Servers (i.e., binding Services to
 * a port).  This class is subclassable. Override copy() and build()
 * to do your own dirty work.
 *
 * Please see the
 * [[https://twitter.github.io/finagle/guide/Configuration.html Finagle user guide]]
 * for information on the preferred `with`-style client-construction APIs.
 *
 * The main class to use is [[com.twitter.finagle.builder.ServerBuilder]], as so
 * {{{
 * ServerBuilder()
 *   .stack(Http.server)
 *   .hostConnectionMaxLifeTime(5.minutes)
 *   .readTimeout(2.minutes)
 *   .name("servicename")
 *   .bindTo(new InetSocketAddress(serverPort))
 *   .build(plusOneService)
 * }}}
 *
 * The `ServerBuilder` requires the definition of `stack`, `bindTo`
 * and `name`. In Scala, these are statically type
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
 * ServerBuilder.safeBuild(
 *  plusOneService,
 *  ServerBuilder.get()
 *   .stack(Http.server())
 *   .hostConnectionMaxLifeTime(5.minutes)
 *   .readTimeout(2.minutes)
 *   .name("servicename")
 *   .bindTo(new InetSocketAddress(serverPort)));
 * }}}
 *
 * Alternatively, using the `unsafeBuild` method on `ServerBuilder`
 * verifies the builder dynamically, resulting in a runtime error
 * instead of a compiler error.
 *
 * =Defaults=
 *
 * The following defaults are applied to servers constructed via ServerBuilder,
 * unless overridden with the corresponding method. These defaults were chosen
 * carefully so as to work well for most use cases. Before changing any of them,
 * make sure that you know exactly how they will affect your application --
 * these options are typically only changed by expert users.
 *
 * - `openConnectionsThresholds`: None
 * - `maxConcurrentRequests`: Int.MaxValue
 * - `backlog`: OS-defined default value
 *
 * @see The [[https://twitter.github.io/finagle/guide/Configuration.html user guide]]
 *      for information on the preferred `with`-style APIs instead.
 */
@deprecated("Use the stack server pattern instead", "2021-06-21")
class ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName] private[builder] (
  private[finagle] val server: StackBasedServer[Req, Rep]) {
  import ServerConfig._
  import com.twitter.finagle.param._

  // Convenient aliases.
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
  type ThisConfig = ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName]
  type This = ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName]

  private[builder] def this() = this(ServerConfig.nilServer)

  override def toString: String = "ServerBuilder(%s)".format(params)

  private def copy[Req1, Rep1, HasCodec1, HasBindTo1, HasName1](
    server: StackBasedServer[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, HasCodec1, HasBindTo1, HasName1] =
    new ServerBuilder(server)

  private def _configured[P, HasCodec1, HasBindTo1, HasName1](
    param: P
  )(
    implicit stackParam: Stack.Param[P]
  ): ServerBuilder[Req, Rep, HasCodec1, HasBindTo1, HasName1] =
    copy(server.configured(param))

  /**
   * Configure the underlying [[Stack.Param Params]].
   *
   * @param param   Configures the server with a given `param`
   *
   * Java users may find it easier to use the `Tuple2` version below.
   */
  def configured[P](param: P)(implicit stackParam: Stack.Param[P]): This =
    copy(server.configured(param))

  /**
   * Java friendly API for configuring the underlying [[Stack.Param Params]].
   *
   * @param paramAndStackParam  Configures the server with a given param represented
   *                            as the tuple `(param value, stack param instance)`
   *
   * The `Tuple2` can often be created by calls to a `mk(): (P, Stack.Param[P])`
   * method on parameters (see
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory.Param.mk()]]
   * as an example).
   */
  def configured[P](paramAndStackParam: (P, Stack.Param[P])): This =
    copy(server.configured(paramAndStackParam._1)(paramAndStackParam._2))

  /**
   * The underlying [[Stack.Param Params]] used for configuration.
   */
  def params: Stack.Params = server.params

  /**
   * Overrides the stack and [[com.twitter.finagle.Server]] that will be used
   * by this builder.
   *
   * @param server A [[StackBasedServer]] representation of a
   * [[com.twitter.finagle.Server]]. `server` is materialized with the state of
   * configuration when `build` is called. There is no guarantee that all
   * builder parameters will be used by the resultant `Server`; it is up to the
   * discretion of `server` itself and the protocol implementation.
   */
  def stack[Req1, Rep1](
    server: StackBasedServer[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    copy(server.withParams(server.params ++ params))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withStatsReceiver`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withStatsReceiver(receiver)
   * }}}
   */
  def reportTo(receiver: StatsReceiver): This =
    _configured(Stats(receiver))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withLabel`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withLabel("my_server")
   * }}}
   */
  def name(value: String): ServerBuilder[Req, Rep, HasCodec, HasBindTo, Yes] =
    _configured(Label(value))

  /**
   * To migrate to the Stack-based APIs, use `ServerTransportParams.sendBufferSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.sendBufferSize(value)
   * }}}
   */
  def sendBufferSize(value: Int): This =
    _configured(params[Transport.BufferSizes].copy(send = Some(value)))

  /**
   * To migrate to the Stack-based APIs, use `ServerTransportParams.receiveBufferSize`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.receiveBufferSize(value)
   * }}}
   */
  def recvBufferSize(value: Int): This =
    _configured(params[Transport.BufferSizes].copy(recv = Some(value)))

  def backlog(value: Int): This =
    _configured(Listener.Backlog(Some(value)))

  /**
   * To migrate to the Stack-based APIs, use `Server.serve`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.Service
   *
   * val service: Service[Req, Rep] = ???
   * Http.server.serve(address, service)
   * }}}
   */
  def bindTo(address: SocketAddress): ServerBuilder[Req, Rep, HasCodec, Yes, HasName] =
    _configured(BindTo(address))

  /**
   * To migrate to the Stack-based APIs, use `ServerTransportParams.verbose`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.verbose
   * }}}
   */
  def logChannelActivity(v: Boolean): This =
    _configured(Transport.Verbose(v))

  /**
   * Encrypt the connection with SSL/TLS.
   *
   * To migrate to the Stack-based APIs, use `ServerTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.tls(config)
   * }}}
   */
  def tls(config: SslServerConfiguration): This =
    configured(Transport.ServerSsl(Some(config)))

  /**
   * Encrypt the connection with SSL/TLS.
   *
   * To migrate to the Stack-based APIs, use `ServerTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.tls(config, engineFactory)
   * }}}
   */
  def tls(config: SslServerConfiguration, engineFactory: SslServerEngineFactory): This =
    configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerEngineFactory.Param(engineFactory))

  /**
   * Encrypt the connection with SSL/TLS.
   *
   * To migrate to the Stack-based APIs, use `ServerTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.tls(config, sessionVerifier)
   * }}}
   */
  def tls(config: SslServerConfiguration, sessionVerifier: SslServerSessionVerifier): This =
    configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerSessionVerifier.Param(sessionVerifier))

  /**
   * Encrypt the connection with SSL/TLS.
   *
   * To migrate to the Stack-based APIs, use `ServerTransportParams.tls`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.tls(config, engineFactory, sessionVerifier)
   * }}}
   */
  def tls(
    config: SslServerConfiguration,
    engineFactory: SslServerEngineFactory,
    sessionVerifier: SslServerSessionVerifier
  ): This =
    configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerEngineFactory.Param(engineFactory))
      .configured(SslServerSessionVerifier.Param(sessionVerifier))

  /**
   * Configures the maximum concurrent requests that are admitted
   * by the server at any given time. If the server receives a
   * burst of traffic that exceeds this limit, the burst is rejected
   * with a `FailureFlags.Rejected` exception. Note, this failure signals
   * a graceful rejection which is transmitted to clients by certain
   * protocols in Finagle (e.g. Http, ThriftMux). The limit is global
   * to all sessions.
   *
   * To migrate to the Stack-based APIs, use `ServerAdmissionControlParams.concurrencyLimit`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withAdmissionControl.concurrencyLimit(10, 0)
   * }}}
   */
  def maxConcurrentRequests(max: Int): This = {
    _configured(PendingRequestFilter.Param(Some(max)))
  }

  /**
   * Configure admission control filters in the server Stack.
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.filter.ServerAdmissionControl
   *
   * Http.server.configured(ServerAdmissionControl.Param(enable))
   * }}}
   *
   * @see [[com.twitter.finagle.filter.ServerAdmissionControl]]
   */
  def enableAdmissionControl(enable: Boolean): This =
    _configured(ServerAdmissionControl.Param(enable))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withRequestTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withRequestTimeout(howlong)
   * }}}
   *
   * @note if the request is not complete after `howlong`, the work that is
   *       in progress will be interrupted via [[Future.raise]].
   */
  def requestTimeout(howlong: Duration): This =
    _configured(TimeoutFilter.Param(howlong))

  def keepAlive(value: Boolean): This =
    _configured(params[Transport.Liveness].copy(keepAlive = Some(value)))

  /**
   * To migrate to the Stack-based APIs, use `TransportParams.readTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.readTimeout(howlong)
   * }}}
   */
  def readTimeout(howlong: Duration): This =
    _configured(params[Transport.Liveness].copy(readTimeout = howlong))

  /**
   * To migrate to the Stack-based APIs, use `TransportParams.writeTimeout`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTransport.writeTimeout(howlong)
   * }}}
   */
  def writeCompletionTimeout(howlong: Duration): This =
    _configured(params[Transport.Liveness].copy(writeTimeout = howlong))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withMonitor`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.util.Monitor
   *
   * val monitor: Monitor = ???
   * Http.server.withMonitor(monitor)
   * }}}
   */
  def monitor(mFactory: (String, SocketAddress) => util.Monitor): This =
    _configured(MonitorFactory(mFactory))

  /**
   * To migrate to the Stack-based APIs, use `CommonParams.withTracer`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withTracer(t)
   * }}}
   */
  def tracer(t: com.twitter.finagle.tracing.Tracer): This =
    _configured(Tracer(t))

  /**
   * Cancel pending futures whenever the the connection is shut down.
   * This defaults to true.
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.filter.MaskCancelFilter
   *
   * Http.server.configured(MaskCancelFilter.Param(!yesOrNo))
   * }}}
   *
   * @see [[com.twitter.finagle.filter.ServerAdmissionControl]]
   */
  def cancelOnHangup(yesOrNo: Boolean): This = {
    // Note: We invert `yesOrNo` as the param here because the filter's
    // cancellation-masking is the inverse operation of cancelling on hangup.
    _configured(MaskCancelFilter.Param(!yesOrNo))
  }

  def hostConnectionMaxIdleTime(howlong: Duration): This =
    _configured(params[ExpiringService.Param].copy(idleTime = howlong))

  def hostConnectionMaxLifeTime(howlong: Duration): This =
    _configured(params[ExpiringService.Param].copy(lifeTime = howlong))

  /**
   * Configures the traffic class.
   *
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.server.Listener
   *
   * Http.server.configured(Listener.TrafficClass(value))
   * }}}
   *
   * @see [[Listener.TrafficClass]]
   */
  def trafficClass(value: Option[Int]): This =
    _configured(Listener.TrafficClass(value))

  /**
   * When true, the server is daemonized. As with java threads, a
   * process can only exit only when all remaining servers are daemonized.
   * False by default.
   *
   * The default for the Stack-based APIs is for the server to
   * be daemonized.
   */
  def daemon(daemonize: Boolean): This =
    _configured(Daemonize(daemonize))

  /**
   * Configure a [[com.twitter.finagle.service.ResponseClassifier]]
   * which is used to determine the result of a request/response.
   *
   * This allows developers to give Finagle the additional application-specific
   * knowledge necessary in order to properly classify responses. Without this,
   * Finagle cannot make judgements about application-level failures as it only
   * has a narrow understanding of failures (for example: transport level, timeouts,
   * and nacks).
   *
   * As an example take an HTTP server that returns a response with a 500 status
   * code. To Finagle this is a successful request/response. However, the application
   * developer may want to treat all 500 status codes as failures and can do so via
   * setting a [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * ResponseClassifier is a [[PartialFunction]] and as such multiple classifiers can
   * be composed together via [[PartialFunction.orElse]].
   *
   * Response classification is independently configured on the client and server.
   * For client-side response classification using [[com.twitter.finagle.builder.ClientBuilder]],
   * see `com.twitter.finagle.builder.ClientBuilder.responseClassifier`
   *
   * To migrate to the Stack-based APIs, use `CommonParams.withResponseClassifier`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withResponseClassifier(classifier)
   * }}}
   *
   * @see [[com.twitter.finagle.http.service.HttpResponseClassifier]] for some
   * HTTP classification tools.
   *
   * @note If unspecified, the default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]]
   * which is a total function fully covering the input domain.
   */
  def responseClassifier(classifier: com.twitter.finagle.service.ResponseClassifier): This =
    _configured(param.ResponseClassifier(classifier))

  /**
   * The currently configured [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * @note If unspecified, the default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]].
   */
  def responseClassifier: com.twitter.finagle.service.ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier

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
   * Http.server.withExceptionStatsHandler(exceptionStatsHandler)
   * }}}
   *
   * @param exceptionStatsHandler function to record failure details.
   */
  def exceptionCategorizer(exceptionStatsHandler: stats.ExceptionStatsHandler): This =
    _configured(ExceptionStatsHandler(exceptionStatsHandler))

  /* Builder methods follow */

  /**
   * Construct the Server, given the provided Service.
   */
  def build(
    service: Service[Req, Rep]
  )(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION: ServerConfigEvidence[
      HasCodec,
      HasBindTo,
      HasName
    ]
  ): ListeningServer = build(ServiceFactory.const(service))

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  def build(
    serviceFactory: ServiceFactory[Req, Rep]
  )(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION: ServerConfigEvidence[
      HasCodec,
      HasBindTo,
      HasName
    ]
  ): ListeningServer = {

    val Label(lbl) = params[Label]
    val label = if (lbl == "") "server" else lbl

    val BindTo(addr) = params[BindTo]
    val logger = DefaultLogger
    val Daemonize(daemon) = params[Daemonize]
    val MonitorFactory(newMonitor) = params[MonitorFactory]
    val statsReceiver = new RoleConfiguredStatsReceiver(
      new RelativeNameMarkingStatsReceiver(params[Stats].statsReceiver),
      Server,
      Some(label))

    val monitor = newMonitor(lbl, InetSocketAddressUtil.toPublic(addr)) andThen
      new SourceTrackingMonitor(logger, label)

    val serverParams = params +
      Monitor(monitor) +
      Reporter(NullReporterFactory) +
      Stats(statsReceiver)

    val listeningServer = server.withParams(serverParams).serve(addr, serviceFactory)

    new ListeningServer with CloseAwaitably {

      val exitGuard = if (!daemon) Some(ExitGuard.guard(s"server for '$label'")) else None

      override protected def closeServer(deadline: Time): Future[Unit] = closeAwaitably {
        listeningServer.close(deadline) ensure {
          exitGuard.foreach(_.unguard())
        }
      }

      override def boundAddress: SocketAddress = listeningServer.boundAddress
    }
  }

  /**
   * Construct a Service, with runtime checks for builder
   * completeness.
   */
  def unsafeBuild(service: Service[Req, Rep]): ListeningServer = {
    if (!params.contains[BindTo])
      throw new IncompleteSpecification("No bindTo was specified")

    if (!params.contains[Label])
      throw new IncompleteSpecification("No name were specified")

    val sb = this.asInstanceOf[ServerBuilder[Req, Rep, Yes, Yes, Yes]]
    sb.build(service)
  }
}
