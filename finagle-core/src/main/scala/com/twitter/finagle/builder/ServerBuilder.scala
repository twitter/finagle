package com.twitter.finagle.builder

import com.twitter.util
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.{Server => FinagleServer, _}
import com.twitter.finagle.filter.{MaskCancelFilter, RequestSemaphoreFilter, ServerAdmissionControl}
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server.{Listener, StackBasedServer, StackServer, StdStackServer}
import com.twitter.finagle.service.{ExpiringService, TimeoutFilter}
import com.twitter.finagle.ssl.{ApplicationProtocols, CipherSuites, Engine, KeyCredentials}
import com.twitter.finagle.ssl.server.{
  ConstServerEngineFactory, SslServerConfiguration, SslServerEngineFactory}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.util.{CloseAwaitably, Duration, Future, NullMonitor, Time}
import java.io.File
import java.net.SocketAddress
import javax.net.ssl.SSLEngine
import org.jboss.netty.channel.ServerChannelFactory
import scala.annotation.implicitNotFound

/**
 * A listening server. This is for compatibility with older code that is
 * using builder.Server. New code should use the ListeningServer trait.
 */
trait Server extends ListeningServer {

  /**
   * When a server is bound to an ephemeral port, gets back the address
   * with concrete listening port picked.
   */
  @deprecated("Use boundAddress", "2014-12-19")
  final def localAddress: SocketAddress = boundAddress
}

/**
 * Factory for [[com.twitter.finagle.builder.ServerBuilder]] instances
 */
object ServerBuilder {

  type Complete[Req, Rep] = ServerBuilder[
    Req, Rep, ServerConfig.Yes,
    ServerConfig.Yes, ServerConfig.Yes]

  def apply(): ServerBuilder[Nothing, Nothing, Nothing, Nothing, Nothing] =
    new ServerBuilder()
  def get(): ServerBuilder[Nothing, Nothing, Nothing, Nothing, Nothing] =
    apply()

  /**
   * Provides a typesafe `build` for Java.
   */
  def safeBuild[Req, Rep](service: Service[Req, Rep], builder: Complete[Req, Rep]): Server =
    builder.build(service)(ServerConfigEvidence.FullyConfigured)

  /**
   * Provides a typesafe `build` for Java.
   */
  def safeBuild[Req, Rep](
    serviceFactory: ServiceFactory[Req, Rep],
    builder: Complete[Req, Rep]
  ): Server =
    builder.build(serviceFactory)(ServerConfigEvidence.FullyConfigured)
}

object ServerConfig {
  sealed trait Yes
  type FullySpecified[Req, Rep] = ServerConfig[Req, Rep, Yes, Yes, Yes]

  def nilServer[Req, Rep]: FinagleServer[Req, Rep] = new FinagleServer[Req, Rep] {
    def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer =
      NullServer
  }

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

@implicitNotFound("Builder is not fully configured: Codec: ${HasCodec}, BindTo: ${HasBindTo}, Name: ${HasName}")
trait ServerConfigEvidence[HasCodec, HasBindTo, HasName]

private[builder] object ServerConfigEvidence {
  implicit object FullyConfigured extends ServerConfigEvidence[ServerConfig.Yes, ServerConfig.Yes, ServerConfig.Yes]
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
 *   .codec(Http)
 *   .hostConnectionMaxLifeTime(5.minutes)
 *   .readTimeout(2.minutes)
 *   .name("servicename")
 *   .bindTo(new InetSocketAddress(serverPort))
 *   .build(plusOneService)
 * }}}
 *
 * The `ServerBuilder` requires the definition of `codec`, `bindTo`
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
 *   .codec(Http)
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
 *      for information on the preferred `with`-style APIs insead.
 */
class ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName] private[builder](
  val params: Stack.Params,
  mk: Stack.Params => FinagleServer[Req, Rep]
) {
  import ServerConfig._
  import com.twitter.finagle.param._

  // Convenient aliases.
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
  type ThisConfig           = ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName]
  type This                 = ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName]

  private[builder] def this() = this(Stack.Params.empty, Function.const(ServerConfig.nilServer)_)

  override def toString: String = "ServerBuilder(%s)".format(params)

  protected def copy[Req1, Rep1, HasCodec1, HasBindTo1, HasName1](
    ps: Stack.Params,
    newServer: Stack.Params => FinagleServer[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, HasCodec1, HasBindTo1, HasName1] =
    new ServerBuilder(ps, newServer)

  private def _configured[P, HasCodec1, HasBindTo1, HasName1](
    param: P
  )(
    implicit stackParam: Stack.Param[P]
  ): ServerBuilder[Req, Rep, HasCodec1, HasBindTo1, HasName1] =
    copy(params + param, mk)

  /**
   * Configure the underlying [[Stack.Param Params]].
   *
   * @param param   Configures the server with a given `param`
   *
   * Java users may find it easier to use the `Tuple2` version below.
   */
  def configured[P](param: P)(implicit stackParam: Stack.Param[P]): This =
    copy(params + param, mk)

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
    copy(params.+(paramAndStackParam._1)(paramAndStackParam._2), mk)

  /**
   * To migrate to the Stack-based APIs, use `ServerBuilder.stack(Protocol.server)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ServerBuilder().stack(Http.server)
   * }}}
   */
  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    this.codec((_: ServerCodecConfig) => codec)
      ._configured(ProtocolLibrary(codec.protocolLibraryName))

  /**
   * To migrate to the Stack-based APIs, use `ServerBuilder.stack(Protocol.server)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ServerBuilder().stack(Http.server)
   * }}}
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    this.codec(codecFactory.server)
      ._configured(ProtocolLibrary(codecFactory.protocolLibraryName))

  /**
   * To migrate to the Stack-based APIs, use `ServerBuilder.stack(Protocol.server)`
   * instead. For example:
   * {{{
   * import com.twitter.finagle.Http
   *
   * ServerBuilder().stack(Http.server)
   * }}}
   */
  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Server
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    stack({ ps =>
      val Label(label) = ps[Label]
      val BindTo(addr) = ps[BindTo]
      val Stats(stats) = ps[Stats]
      val codec = codecFactory(ServerCodecConfig(label, addr))


      val newStack = StackServer.newStack[Req1, Rep1].replace(
        StackServer.Role.preparer, (next: ServiceFactory[Req1, Rep1]) =>
          codec.prepareConnFactory(next, ps + Stats(stats.scope(label)))
      ).replace(TraceInitializerFilter.role, codec.newTraceInitializer)

      case class Server(
        stack: Stack[ServiceFactory[Req1, Rep1]] = newStack,
        params: Stack.Params = ps
      ) extends StdStackServer[Req1, Rep1, Server] {
        protected type In = Any
        protected type Out = Any

        protected def copy1(
          stack: Stack[ServiceFactory[Req1, Rep1]] = this.stack,
          params: Stack.Params = this.params
        ) = copy(stack, params)

        protected def newListener(): Listener[Any, Any] =
          Netty3Listener(codec.pipelineFactory, params)

        protected def newDispatcher(transport: Transport[In, Out], service: Service[Req1, Rep1]) =
          codec.newServerDispatcher(transport, service)
      }

      val proto = ps[ProtocolLibrary]
      val serverParams =
        if (proto != ProtocolLibrary.param.default) ps
        else ps + ProtocolLibrary(codec.protocolLibraryName)

      Server(
        stack = newStack,
        params = serverParams
      )
    })

  /**
   * Overrides the stack and [[com.twitter.finagle.Server]] that will be used
   * by this builder.
   *
   * @param mk A function that materializes a `Server` from a set of `Params`.
   * `mk` is passed the state of configuration when `build` is called. There is
   * no guarantee that all the builder parameters will be used by the server
   * created by `mk`; it is up to the discretion of the server and protocol
   * implementation.
   */
  @deprecated("Use stack(server: StackBasedServer)", "7.0.0")
  def stack[Req1, Rep1](
    mk: Stack.Params => FinagleServer[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    copy(params, mk)

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
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] = {
    val withParams: Stack.Params => FinagleServer[Req1, Rep1] = { ps =>
      server.withParams(server.params ++ ps)
    }
    copy(params, withParams)
  }

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

  @deprecated("use com.twitter.finagle.netty3.numWorkers flag instead", "2015-11-18")
  def channelFactory(cf: ServerChannelFactory): This =
    _configured(Netty3Listener.ChannelFactory(cf))

  /**
   * To migrate to the Stack-based APIs, use `configured`.
   * For example:
   * {{{
   * import com.twitter.finagle.Http
   * import com.twitter.finagle.param
   *
   * Http.server.configured(param.Logger(logger))
   * }}}
   */
  def logger(logger: java.util.logging.Logger): This =
    _configured(Logger(logger))

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
   * Http.server.withTransport.tls(...)
   * }}}
   */
  def tls(
    certificatePath: String,
    keyPath: String,
    caCertificatePath: String = null,
    ciphers: String = null,
    nextProtos: String = null
  ): This = {
    val keyCredentials = if (caCertificatePath == null) {
      KeyCredentials.CertAndKey(new File(certificatePath), new File(keyPath))
    } else {
      KeyCredentials.CertKeyAndChain(
        new File(certificatePath), new File(keyPath), new File(caCertificatePath))
    }
    val cipherSuites = if (ciphers == null) CipherSuites.Unspecified
      else CipherSuites.fromString(ciphers)
    val applicationProtocols = if (nextProtos == null) ApplicationProtocols.Unspecified
      else ApplicationProtocols.fromString(nextProtos)

    configured(Transport.ServerSsl(
      Some(SslServerConfiguration(
        keyCredentials = keyCredentials,
        cipherSuites = cipherSuites,
        applicationProtocols = applicationProtocols))))
  }

  /**
   * Provide a raw SSL engine that is used to establish SSL sessions.
   */
  def newSslEngine(newSsl: () => SSLEngine): This =
    newFinagleSslEngine(() => new Engine(newSsl()))

  def newFinagleSslEngine(v: () => Engine): This =
    _configured(SslServerEngineFactory.Param(
      new ConstServerEngineFactory(v)))
    .configured(Transport.ServerSsl(
      Some(SslServerConfiguration())))

  /**
   * Configures the maximum concurrent requests that are admitted
   * by the server at any given time. If the server receives a
   * burst of traffic that exceeds this limit, the burst is rejected
   * with a `Failure.Rejected` exception. Note, this failure signals
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
    val sem =
      if (max == Int.MaxValue) None
      else Some(new AsyncSemaphore(max, 0))

    _configured(RequestSemaphoreFilter.Param(sem))
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

  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(factory: com.twitter.finagle.tracing.Tracer.Factory): This =
    tracer(factory())

  // API compatibility method
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(t: com.twitter.finagle.tracing.Tracer): This =
    tracer(t)

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
  def build(service: Service[Req, Rep]) (
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ServerConfigEvidence[HasCodec, HasBindTo, HasName]
   ): Server = build(ServiceFactory.const(service))

  @deprecated("Used for ABI compat", "5.0.1")
  final def build(service: Service[Req, Rep],
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
   ): Server = build(ServiceFactory.const(service), THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION)

  /**
   * Construct the Server, given the provided Service factory.
   */
  @deprecated("Use the ServiceFactory variant instead", "5.0.1")
  final def build(serviceFactory: () => Service[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build((_:ClientConnection) => serviceFactory())(THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION)

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  @deprecated("Use the ServiceFactory variant instead", "5.0.1")
  final def build(serviceFactory: (ClientConnection) => Service[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build(new ServiceFactory[Req, Rep] {
    def apply(conn: ClientConnection) = Future.value(serviceFactory(conn))
    def close(deadline: Time) = Future.Done
  }, THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION)

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  def build(serviceFactory: ServiceFactory[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ServerConfigEvidence[HasCodec, HasBindTo, HasName]
  ): Server = {

    val Label(lbl) = params[Label]
    val label = if (lbl == "") "server" else lbl

    val BindTo(addr) = params[BindTo]
    val Logger(logger) = params[Logger]
    val Daemonize(daemon) = params[Daemonize]
    val MonitorFactory(newMonitor) = params[MonitorFactory]

    val monitor = newMonitor(lbl, InetSocketAddressUtil.toPublic(addr)) andThen
      new SourceTrackingMonitor(logger, label)

    val serverParams = params +
      Monitor(monitor) +
      Reporter(NullReporterFactory)

    val listeningServer = mk(serverParams).serve(addr, serviceFactory)

    new Server with CloseAwaitably {

      val exitGuard = if (!daemon) Some(ExitGuard.guard(s"server for '$label'")) else None

      override protected def closeServer(deadline: Time): Future[Unit] = closeAwaitably {
        listeningServer.close(deadline) ensure {
          exitGuard.foreach(_.unguard())
        }
      }

      override def boundAddress: SocketAddress = listeningServer.boundAddress
    }
  }

  @deprecated("Used for ABI compat", "5.0.1")
  final def build(serviceFactory: ServiceFactory[Req, Rep],
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build(serviceFactory)(
    new ServerConfigEvidence[HasCodec, HasBindTo, HasName]{})

  /**
   * Construct a Service, with runtime checks for builder
   * completeness.
   */
  def unsafeBuild(service: Service[Req, Rep]): Server = {
    if (!params.contains[BindTo])
      throw new IncompleteSpecification("No bindTo was specified")

    if (!params.contains[Label])
      throw new IncompleteSpecification("No name were specified")

    val sb = this.asInstanceOf[ServerBuilder[Req, Rep, Yes, Yes, Yes]]
    sb.build(service)
  }
}
