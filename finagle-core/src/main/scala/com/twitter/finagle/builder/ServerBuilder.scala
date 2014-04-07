package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.channel.IdleConnectionFilter
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.finagle.dispatch.ExpiringServerDispatcher
import com.twitter.finagle.filter.{MaskCancelFilter, RequestSemaphoreFilter}
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server.{Listener, NullListener, StackServer}
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.ssl.{Ssl, Engine}
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.util.{Closable, Duration, Future, NullMonitor, Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import javax.net.ssl.SSLEngine
import org.jboss.netty.channel.ServerChannelFactory
import scala.annotation.implicitNotFound

/**
 * A listening server.
 */
trait Server extends Closable {
  /**
   * When a server is bound to an ephemeral port, gets back the address
   * with concrete listening port picked.
   */
  def localAddress: SocketAddress
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
  sealed abstract trait Yes
  type FullySpecified[Req, Rep] = ServerConfig[Req, Rep, Yes, Yes, Yes]

  def nilServer[Req, Rep] = new StackServer[Req, Rep, Any, Any](nilStack, Stack.Params.empty) {
    val newListener: Stack.Params => Listener[Any, Any] = Function.const(NullListener)(_)
    val newDispatcher = (_: Any, _: Any) => Closable.nop
  }

  // params specific to ServerBuilder
  case class BindTo(addr: SocketAddress)
  implicit object BindTo extends Stack.Param[BindTo] {
    val default = BindTo(new SocketAddress {
      override val toString = "unknown"
    })
  }

  case class CancelOnHangup(yesOrNo: Boolean)
  implicit object CancelOnHangup extends Stack.Param[CancelOnHangup] {
    val default = CancelOnHangup(true)
  }

  case class MonitorFactory(mFactory: (String, SocketAddress) => com.twitter.util.Monitor)
  implicit object MonitorFactory extends Stack.Param[MonitorFactory] {
    val default = MonitorFactory((_, _) => NullMonitor)
  }

  case class Daemonize(onOrOff: Boolean)
  implicit object Daemonize extends Stack.Param[Daemonize] {
    val default = Daemonize(false)
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
 */
class ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName] private[builder](
  params: Stack.Params,
  mk: Stack.Params => StackServer[Req, Rep, Any, Any]
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
    newServer: Stack.Params => StackServer[Req1, Rep1, Any, Any]
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

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    this.codec(codecFactory.server)

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Server
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    stack({ ps =>
      val Label(label) = ps[Label]
      val BindTo(addr) = ps[BindTo]
      val codec = codecFactory(ServerCodecConfig(label, addr))
      val newStack = StackServer.newStack[Req1, Rep1].replace(
        StackServer.Role.Preparer, (next: ServiceFactory[Req1, Rep1]) =>
          codec.prepareConnFactory(next)
      )
      new StackServer[Req1, Rep1, Any, Any](newStack, ps) {
        protected val newListener: Stack.Params => Listener[Any, Any] = { prms =>
          Netty3Listener(codec.pipelineFactory, prms)
        }

        protected val newDispatcher = {
          // TODO: Expiration logic should be installed using ExpiringService
          // in StackServer#newStack. Then we can thread through "closes"
          // via ClientConnection.
          val Timer(timer) = ps[Timer]
          val ExpiringService.Param(idleTime, lifeTime) = ps[ExpiringService.Param]
          val Stats(sr) = ps[Stats]
          val idle = if (idleTime.isFinite) Some(idleTime) else None
          val life = if (lifeTime.isFinite) Some(lifeTime) else None
          val f = codec.newServerDispatcher(_, _)
          (idle, life) match {
            case (None, None) => f
            case _ => ExpiringServerDispatcher[Req1, Rep1, Any, Any](
              idle, life, timer, sr.scope("expired"), f)
          }
        }
      }
    })

  def stack[Req1, Rep1](
    mk: Stack.Params => StackServer[Req1, Rep1, Any, Any]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    copy(params, mk)

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
    configured(Transport.TLSEngine(Some(v)))

  def maxConcurrentRequests(max: Int): This =
    configured(RequestSemaphoreFilter.Param(max))

  def requestTimeout(howlong: Duration): This =
    configured(TimeoutFilter.Param(howlong))

  def keepAlive(value: Boolean): This =
    configured(params[Transport.Liveness].copy(keepAlive = Some(value)))

  def readTimeout(howlong: Duration): This =
    configured(params[Transport.Liveness].copy(readTimeout = howlong))

  def writeCompletionTimeout(howlong: Duration): This =
    configured(params[Transport.Liveness].copy(writeTimeout = howlong))

  def monitor(mFactory: (String, SocketAddress) => com.twitter.util.Monitor): This =
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
  def cancelOnHangup(yesOrNo: Boolean): This =
    configured(CancelOnHangup(yesOrNo))

  def hostConnectionMaxIdleTime(howlong: Duration): This =
    configured(params[ExpiringService.Param].copy(idleTime = howlong))

  def hostConnectionMaxLifeTime(howlong: Duration): This =
    configured(params[ExpiringService.Param].copy(lifeTime = howlong))

  def openConnectionsThresholds(thresholds: OpenConnectionsThresholds): This =
    configured(IdleConnectionFilter.Param(Some(thresholds)))

  /**
   * When true, the server is daemonized. As with java threads, a
   * process can only exit only when all remaining servers are daemonized.
   * False by default.
   */
  def daemon(daemonize: Boolean): This =
    configured(Daemonize(daemonize))

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
  ): Server = new Server {
    val Label(label) = params[Label]
    val BindTo(addr) = params[BindTo]
    val Stats(sr) = params[Stats]
    val Logger(logger) = params[Logger]
    val Daemonize(daemon) = params[Daemonize]
    val MonitorFactory(newMonitor) = params[MonitorFactory]

    val monitor = newMonitor(label, InetSocketAddressUtil.toPublic(addr)) andThen
      new SourceTrackingMonitor(logger, "server")
    val statsReceiver = if (label.isEmpty) sr else sr.scope(label)

    val server = mk(params) transformed { stk =>
      params[CancelOnHangup] match {
        case CancelOnHangup(false) => stk.replace(StackServer.Role.MaskCancel,
          new MaskCancelFilter[Req, Rep])
        case _ => stk
      }
    }

    val listeningServer = server
      .configured(Monitor(monitor))
      .configured(Stats(statsReceiver))
      .configured(Reporter(NullReporterFactory))
      .serve(addr, serviceFactory)

    val closed = new AtomicBoolean(false)

    if (!daemon) ExitGuard.guard()
    def close(deadline: Time): Future[Unit] = {
      if (!closed.compareAndSet(false, true)) {
        logger.log(Level.WARNING, "Server closed multiple times!",
          new Exception/*stack trace please*/)
        return Future.exception(new IllegalStateException)
      }

      listeningServer.close(deadline) ensure {
        if (!daemon) ExitGuard.unguard()
      }
    }

    val localAddress = listeningServer.boundAddress
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
    if (mk(params).stack.tails.size == 1)
      throw new IncompleteSpecification("No codec was specified")

    if (!params.contains[BindTo])
      throw new IncompleteSpecification("No bindTo was specified")

    if (!params.contains[Label])
      throw new IncompleteSpecification("No name were specified")

    val sb = this.asInstanceOf[ServerBuilder[Req, Rep, Yes, Yes, Yes]]
    sb.build(service)
  }
}
