package com.twitter.finagle.builder

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.filter.{MaskCancelFilter, RequestSemaphoreFilter}
import com.twitter.finagle.netty3.channel.IdleConnectionFilter
import com.twitter.finagle.netty3.channel.OpenConnectionsThresholds
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.server.{StackBasedServer, Listener, StackServer, StdStackServer}
import com.twitter.finagle.service.{ExpiringService, TimeoutFilter}
import com.twitter.finagle.ssl.{Ssl, Engine}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.finagle.{Server => FinagleServer, _}
import com.twitter.util
import com.twitter.util.{CloseAwaitably, Duration, Future, NullMonitor, Time}
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

  def apply() = new ServerBuilder()
  def get() = apply()

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

  def nilServer[Req, Rep] = new FinagleServer[Req, Rep] {
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

  override def toString() = "ServerBuilder(%s)".format(params)

  protected def copy[Req1, Rep1, HasCodec1, HasBindTo1, HasName1](
    ps: Stack.Params,
    newServer: Stack.Params => FinagleServer[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, HasCodec1, HasBindTo1, HasName1] =
    new ServerBuilder(ps, newServer)

  protected def configured[P: Stack.Param, HasCodec1, HasBindTo1, HasName1](
    param: P
  ): ServerBuilder[Req, Rep, HasCodec1, HasBindTo1, HasName1] =
    copy(params + param, mk)

  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    this.codec((_: ServerCodecConfig) => codec)
      .configured(ProtocolLibrary(codec.protocolLibraryName))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    this.codec(codecFactory.server)
      .configured(ProtocolLibrary(codecFactory.protocolLibraryName))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Server
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    stack({ ps =>
      val Label(label) = ps[Label]
      val BindTo(addr) = ps[BindTo]
      val codec = codecFactory(ServerCodecConfig(label, addr))
      val newStack = StackServer.newStack[Req1, Rep1].replace(
        StackServer.Role.preparer, (next: ServiceFactory[Req1, Rep1]) =>
          codec.prepareConnFactory(next)
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

        protected def newDispatcher(transport: Transport[In, Out], service: Service[Req1, Rep1]) = {
          // TODO: Expiration logic should be installed using ExpiringService
          // in StackServer#newStack. Then we can thread through "closes"
          // via ClientConnection.
          val Timer(timer) = params[Timer]
          val ExpiringService.Param(idleTime, lifeTime) = params[ExpiringService.Param]
          val Stats(sr) = params[Stats]
          val idle = if (idleTime.isFinite) Some(idleTime) else None
          val life = if (lifeTime.isFinite) Some(lifeTime) else None
          val dispatcher = codec.newServerDispatcher(transport, service)
          (idle, life) match {
            case (None, None) => dispatcher
            case _ =>
              new ExpiringService(service, idle, life, timer, sr.scope("expired")) {
                protected def onExpire() { dispatcher.close(Time.now) }
              }
          }
        }
      }

      val proto = params[ProtocolLibrary]
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

  def reportTo(receiver: StatsReceiver): This =
    configured(Stats(receiver))

  def name(value: String): ServerBuilder[Req, Rep, HasCodec, HasBindTo, Yes] =
    configured(Label(value))

 def sendBufferSize(value: Int): This =
    configured(params[Transport.BufferSizes].copy(send = Some(value)))

  def recvBufferSize(value: Int): This =
    configured(params[Transport.BufferSizes].copy(recv = Some(value)))

  def backlog(value: Int): This =
    configured(Listener.Backlog(Some(value)))

  def bindTo(address: SocketAddress): ServerBuilder[Req, Rep, HasCodec, Yes, HasName] =
    configured(BindTo(address))

  @deprecated("use com.twitter.finagle.netty3.numWorkers flag instead", "2015-11-18")
  def channelFactory(cf: ServerChannelFactory): This =
    configured(Netty3Listener.ChannelFactory(cf))

  def logger(logger: java.util.logging.Logger): This =
    configured(Logger(logger))

  def logChannelActivity(v: Boolean): This =
    configured(Transport.Verbose(v))

  def tls(certificatePath: String, keyPath: String,
          caCertificatePath: String = null, ciphers: String = null, nextProtos: String = null): This =
    newFinagleSslEngine(() => Ssl.server(certificatePath, keyPath, caCertificatePath, ciphers, nextProtos))

  /**
   * Provide a raw SSL engine that is used to establish SSL sessions.
   */
  def newSslEngine(newSsl: () => SSLEngine): This =
    newFinagleSslEngine(() => new Engine(newSsl()))

  def newFinagleSslEngine(v: () => Engine): This =
    configured(Transport.TLSServerEngine(Some(v)))

  /**
   * Configures the maximum concurrent requests that are admitted
   * by the server at any given time. If the server receives a
   * burst of traffic that exceeds this limit, the burst is rejected
   * with a `Failure.Rejected` exception. Note, this failure signals
   * a graceful rejection which is transmitted to clients by certain
   * protocols in Finagle (e.g. Http, ThriftMux).
   */
  def maxConcurrentRequests(max: Int): This = {
    val sem =
      if (max == Int.MaxValue) None
      else Some(new AsyncSemaphore(max, 0))

    configured(RequestSemaphoreFilter.Param(sem))
  }

  def requestTimeout(howlong: Duration): This =
    configured(TimeoutFilter.Param(howlong))

  def keepAlive(value: Boolean): This =
    configured(params[Transport.Liveness].copy(keepAlive = Some(value)))

  def readTimeout(howlong: Duration): This =
    configured(params[Transport.Liveness].copy(readTimeout = howlong))

  def writeCompletionTimeout(howlong: Duration): This =
    configured(params[Transport.Liveness].copy(writeTimeout = howlong))

  def monitor(mFactory: (String, SocketAddress) => util.Monitor): This =
    configured(MonitorFactory(mFactory))

  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(factory: com.twitter.finagle.tracing.Tracer.Factory): This =
    tracer(factory())

  // API compatibility method
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(t: com.twitter.finagle.tracing.Tracer): This =
    tracer(t)

  def tracer(t: com.twitter.finagle.tracing.Tracer): This =
    configured(Tracer(t))

  /**
   * Cancel pending futures whenever the the connection is shut down.
   * This defaults to true.
   */
  def cancelOnHangup(yesOrNo: Boolean): This = {
    // Note: We invert `yesOrNo` as the param here because the filter's
    // cancellation-masking is the inverse operation of cancelling on hangup.
    configured(MaskCancelFilter.Param(!yesOrNo))
  }

  def hostConnectionMaxIdleTime(howlong: Duration): This =
    configured(params[ExpiringService.Param].copy(idleTime = howlong))

  def hostConnectionMaxLifeTime(howlong: Duration): This =
    configured(params[ExpiringService.Param].copy(lifeTime = howlong))

  def openConnectionsThresholds(thresholds: OpenConnectionsThresholds): This =
    configured(IdleConnectionFilter.Param(Some(thresholds)))

  /**
   * Configures the traffic class.
   *
   * @see [[Listener.TrafficClass]]
   */
  def trafficClass(value: Option[Int]): This =
    configured(Listener.TrafficClass(value))

  /**
   * When true, the server is daemonized. As with java threads, a
   * process can only exit only when all remaining servers are daemonized.
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

  /* Builder methods follow */

  /**
   * Construct the Server, given the provided Service.
   */
  def build(service: Service[Req, Rep]) (
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ServerConfigEvidence[HasCodec, HasBindTo, HasName]
   ): Server = build(ServiceFactory.const(service))

  @deprecated("Used for ABI compat", "5.0.1")
  def build(service: Service[Req, Rep],
    THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
   ): Server = build(ServiceFactory.const(service), THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION)

  /**
   * Construct the Server, given the provided Service factory.
   */
  @deprecated("Use the ServiceFactory variant instead", "5.0.1")
  def build(serviceFactory: () => Service[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build((_:ClientConnection) => serviceFactory())(THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION)

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  @deprecated("Use the ServiceFactory variant instead", "5.0.1")
  def build(serviceFactory: (ClientConnection) => Service[Req, Rep])(
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

    val Label(label) = params[Label]
    val BindTo(addr) = params[BindTo]
    val Logger(logger) = params[Logger]
    val Daemonize(daemon) = params[Daemonize]
    val MonitorFactory(newMonitor) = params[MonitorFactory]

    val monitor = newMonitor(label, InetSocketAddressUtil.toPublic(addr)) andThen
      new SourceTrackingMonitor(logger, "server")

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
  def build(serviceFactory: ServiceFactory[Req, Rep],
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
