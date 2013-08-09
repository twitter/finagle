package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.finagle.channel.IdleConnectionFilter
import com.twitter.finagle.dispatch.ExpiringServerDispatcher
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.ssl.{Ssl, Engine}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.{NullTracer, Tracer}
import com.twitter.finagle.util._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Duration, Future, Monitor, NullMonitor,   Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Logger, Level}
import javax.net.ssl.SSLEngine
import org.jboss.netty.channel.ServerChannelFactory
import scala.annotation.implicitNotFound
import scala.collection.mutable

/**
 * A listening server.
 */
trait Server extends Closable {
  /**
   * Close the underlying server gracefully with the given grace
   * period. close() will drain the current channels, waiting up to
   * ``timeout'', after which channels are forcibly closed.
   */
  def close(timeout: Duration = Duration.Top): Future[Unit] =
    close(timeout.fromNow)

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
}

object ServerConfig {
  sealed abstract trait Yes
  type FullySpecified[Req, Rep] = ServerConfig[Req, Rep, Yes, Yes, Yes]
}

@implicitNotFound("Builder is not fully configured: Codec: ${HasCodec}, BindTo: ${HasBindTo}, Name: ${HasName}")
trait ServerConfigEvidence[HasCodec, HasBindTo, HasName]

private[builder] object ServerConfigEvidence {
  implicit object FullyConfigured extends ServerConfigEvidence[ServerConfig.Yes, ServerConfig.Yes, ServerConfig.Yes]
}

private[builder] case class BufferSize(
  send: Option[Int] = None,
  recv: Option[Int] = None
)

private[builder] case class TimeoutConfig(
  hostConnectionMaxIdleTime: Option[Duration] = None,
  hostConnectionMaxLifeTime: Option[Duration] = None,
  requestTimeout: Option[Duration] = None,
  readTimeout: Option[Duration] = None,
  writeCompletionTimeout: Option[Duration] = None
)

/**
 * A configuration object that represents what shall be built.
 */
private[builder] final case class ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName](
  private val _codecFactory:                    Option[CodecFactory[Req, Rep]#Server]    = None,
  private val _statsReceiver:                   Option[StatsReceiver]                    = None,
  private val _monitor:                         Option[(String, SocketAddress) => Monitor] = None,
  private val _name:                            Option[String]                           = None,
  private val _bufferSize:                      BufferSize                               = BufferSize(),
  private val _keepAlive:                       Option[Boolean]                          = None,
  private val _backlog:                         Option[Int]                              = None,
  private val _bindTo:                          Option[SocketAddress]                    = None,
  private val _logger:                          Option[Logger]                           = None,
  private val _newEngine:                       Option[() => Engine]                     = None,
  private val _channelFactory:                  ServerChannelFactory                     = Netty3Listener.channelFactory,
  private val _maxConcurrentRequests:           Option[Int]                              = None,
  private val _timeoutConfig:                   TimeoutConfig                            = TimeoutConfig(),
  private val _tracer:                          Tracer                                   = NullTracer,
  private val _openConnectionsThresholds:       Option[OpenConnectionsThresholds]        = None,
  private val _cancelOnHangup:                  Boolean                                  = true,
  private val _logChannelActivity:              Boolean                                  = false,
  private val _daemon:                          Boolean                                  = false)
{
  import ServerConfig._

  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  lazy val codecFactory               = _codecFactory.get
  val statsReceiver                   = _statsReceiver
  val monitor                         = _monitor
  lazy val name                       = _name.get
  val bufferSize                      = _bufferSize
  val keepAlive                       = _keepAlive
  val backlog                         = _backlog
  lazy val bindTo                     = _bindTo.get
  val logger                          = _logger
  val newEngine                       = _newEngine
  val channelFactory                  = _channelFactory
  val maxConcurrentRequests           = _maxConcurrentRequests
  val hostConnectionMaxIdleTime       = _timeoutConfig.hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime       = _timeoutConfig.hostConnectionMaxLifeTime
  val requestTimeout                  = _timeoutConfig.requestTimeout
  val readTimeout                     = _timeoutConfig.readTimeout
  val writeCompletionTimeout          = _timeoutConfig.writeCompletionTimeout
  val timeoutConfig                   = _timeoutConfig
  val tracer                          = _tracer
  val openConnectionsThresholds       = _openConnectionsThresholds
  val cancelOnHangup                  = _cancelOnHangup
  val logChannelActivity              = _logChannelActivity
  val daemon                          = _daemon

  def toMap = Map(
    "codecFactory"                    -> _codecFactory,
    "statsReceiver"                   -> _statsReceiver,
    "monitor"                         -> _monitor,
    "name"                            -> _name,
    "bufferSize"                      -> _bufferSize,
    "keepAlive"                       -> _keepAlive,
    "backlog"                         -> _backlog,
    "bindTo"                          -> _bindTo,
    "logger"                          -> _logger,
    "newEngine"                       -> _newEngine,
    "channelFactory"                  -> Some(_channelFactory),
    "maxConcurrentRequests"           -> _maxConcurrentRequests,
    "hostConnectionMaxIdleTime"       -> _timeoutConfig.hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime"       -> _timeoutConfig.hostConnectionMaxLifeTime,
    "requestTimeout"                  -> _timeoutConfig.requestTimeout,
    "readTimeout"                     -> _timeoutConfig.readTimeout,
    "writeCompletionTimeout"          -> _timeoutConfig.writeCompletionTimeout,
    "tracer"                          -> Some(_tracer),
    "openConnectionsThresholds"       -> Some(_openConnectionsThresholds),
    "cancelOnHangup"                  -> Some(_cancelOnHangup),
    "logChannelActivity"              -> Some(_logChannelActivity),
    "daemon"                          -> Some(_daemon)
  )

  override def toString = {
    "ServerConfig(%s)".format(
      toMap flatMap {
        case (k, Some(v)) =>
          Some("%s=%s".format(k, v))
        case _ =>
          None
      } mkString(", "))
  }

  def validated: ServerConfig[Req, Rep, Yes, Yes, Yes] = {
    _codecFactory getOrElse { throw new IncompleteSpecification("No codec was specified") }
    _bindTo       getOrElse { throw new IncompleteSpecification("No bindTo was specified") }
    _name         getOrElse { throw new IncompleteSpecification("No name were specified") }
    copy()
  }
}

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
  val config: ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName]
) {
  import ServerConfig._

  // Convenient aliases.
  type FullySpecifiedConfig = FullySpecified[Req, Rep]
  type ThisConfig           = ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName]
  type This                 = ServerBuilder[Req, Rep, HasCodec, HasBindTo, HasName]

  private[builder] def this() = this(new ServerConfig)

  override def toString() = "ServerBuilder(%s)".format(config.toString)

  protected def copy[Req1, Rep1, HasCodec1, HasBindTo1, HasName1](
    config: ServerConfig[Req1, Rep1, HasCodec1, HasBindTo1, HasName1]
  ): ServerBuilder[Req1, Rep1, HasCodec1, HasBindTo1, HasName1] =
    new ServerBuilder(config)

  protected def withConfig[Req1, Rep1, HasCodec1, HasBindTo1, HasName1](
    f: ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName] =>
       ServerConfig[Req1, Rep1, HasCodec1, HasBindTo1, HasName1]
    ): ServerBuilder[Req1, Rep1, HasCodec1, HasBindTo1, HasName1] = copy(f(config))

  def codec[Req1, Rep1](
    codec: Codec[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    withConfig(_.copy(_codecFactory = Some(Function.const(codec) _)))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]#Server
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    withConfig(_.copy(_codecFactory = Some(codecFactory)))

  def codec[Req1, Rep1](
    codecFactory: CodecFactory[Req1, Rep1]
  ): ServerBuilder[Req1, Rep1, Yes, HasBindTo, HasName] =
    withConfig(_.copy(_codecFactory = Some(codecFactory.server)))

  def reportTo(receiver: StatsReceiver): This =
    withConfig(_.copy(_statsReceiver = Some(receiver)))

  def name(value: String): ServerBuilder[Req, Rep, HasCodec, HasBindTo, Yes] =
    withConfig(_.copy(_name = Some(value)))

  def sendBufferSize(value: Int): This =
    withConfig(_.copy(_bufferSize = config.bufferSize.copy(send = Some(value))))

  def recvBufferSize(value: Int): This =
    withConfig(_.copy(_bufferSize = config.bufferSize.copy(recv = Some(value))))

  def keepAlive(value: Boolean): This =
    withConfig(_.copy(_keepAlive = Some(value)))

  def backlog(value: Int): This =
    withConfig(_.copy(_backlog = Some(value)))

  def bindTo(address: SocketAddress): ServerBuilder[Req, Rep, HasCodec, Yes, HasName] =
    withConfig(_.copy(_bindTo = Some(address)))

  def channelFactory(cf: ServerChannelFactory): This =
    withConfig(_.copy(_channelFactory = cf))

  def logger(logger: Logger): This =
    withConfig(_.copy(_logger = Some(logger)))

  def logChannelActivity(v: Boolean): This =
    withConfig(_.copy(_logChannelActivity = v))

  def tls(certificatePath: String, keyPath: String,
          caCertificatePath: String = null, ciphers: String = null, nextProtos: String = null): This =
    newFinagleSslEngine(() => Ssl.server(certificatePath, keyPath, caCertificatePath, ciphers, nextProtos))

  /**
   * Provide a raw SSL engine that is used to establish SSL sessions.
   */
  def newSslEngine(newSsl: () => SSLEngine): This =
    newFinagleSslEngine(() => new Engine(newSsl()))

  def newFinagleSslEngine(v: () => Engine): This =
    withConfig(_.copy(_newEngine = Some(v)))

  def maxConcurrentRequests(max: Int): This =
    withConfig(_.copy(_maxConcurrentRequests = Some(max)))

  def hostConnectionMaxIdleTime(howlong: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(hostConnectionMaxIdleTime = Some(howlong))))

  def hostConnectionMaxLifeTime(howlong: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(hostConnectionMaxLifeTime = Some(howlong))))

  def requestTimeout(howlong: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(requestTimeout = Some(howlong))))

  def readTimeout(howlong: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(readTimeout = Some(howlong))))

  def writeCompletionTimeout(howlong: Duration): This =
    withConfig(c => c.copy(_timeoutConfig = c.timeoutConfig.copy(writeCompletionTimeout = Some(howlong))))

  def monitor(mFactory: (String, SocketAddress) => Monitor): This =
    withConfig(_.copy(_monitor = Some(mFactory)))

  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(factory: Tracer.Factory): This =
    tracer(factory())

  // API compatibility method
  @deprecated("Use tracer() instead", "7.0.0")
  def tracerFactory(t: Tracer): This =
    tracer(t)

  def tracer(t: Tracer): This =
    withConfig(_.copy(_tracer = t))

  /**
   * Cancel pending futures whenever the the connection is shut down.
   * This defaults to true.
   */
  def cancelOnHangup(yesOrNo: Boolean): This =
    withConfig(_.copy(_cancelOnHangup = yesOrNo))

  def openConnectionsThresholds(thresholds: OpenConnectionsThresholds): This =
    withConfig(_.copy(_openConnectionsThresholds = Some(thresholds)))

  /**
   * When true, the server is daemonized. As with java threads, a
   * process can only exit only when all remaining servers are daemonized.
   * False by default.
   */
  def daemon(daemonize: Boolean): This =
    withConfig(_.copy(_daemon = daemonize))

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
    import com.twitter.finagle.server._
    import com.twitter.finagle.netty3._

    val codecConfig = ServerCodecConfig(
      serviceName = config.name, boundAddress = config.bindTo)
    val codec = config.codecFactory(codecConfig)

    val statsReceiver = config.statsReceiver map(_.scope(config.name)) getOrElse NullStatsReceiver
    val logger = config.logger getOrElse Logger.getLogger(config.name)

    val serverAddr = new InetSocketAddress(InetAddress.getLocalHost, 0) //don't care about multi-homed, or port
    val monitor = config.monitor map(_( config.name, serverAddr)) getOrElse NullMonitor

    val tracer = config.tracer
    val timer = DefaultTimer.twitter
    val nettyTimer = DefaultTimer

    val listener = Netty3Listener[Rep, Req](
      name = config.name,
      pipelineFactory = codec.pipelineFactory,
      channelSnooper =
        if (config.logChannelActivity) Some(ChannelSnooper(config.name)(logger.log(Level.INFO, _, _)))
        else None,
      channelFactory = config.channelFactory,
      bootstrapOptions = {
        val o = new mutable.MapBuilder[String, Object, Map[String, Object]](Map())
        o += "soLinger" -> (0: java.lang.Integer)
        o += "reuseAddress" -> java.lang.Boolean.TRUE
        o += "child.tcpNoDelay" -> java.lang.Boolean.TRUE

        for (v <- config.backlog) o += "backlog" -> (v: java.lang.Integer)
        for (v <- config.bufferSize.send) o += "child.sendBufferSize" -> (v: java.lang.Integer)
        for (v <- config.bufferSize.recv) o += "child.receiveBufferSize" -> (v: java.lang.Integer)
        for (v <- config.keepAlive) o += "child.keepAlive" -> (v: java.lang.Boolean)

        o.result()
      },
      channelReadTimeout = config.readTimeout getOrElse Duration.Top,
      channelWriteCompletionTimeout = config.writeCompletionTimeout getOrElse Duration.Top,
      tlsConfig = config.newEngine map(Netty3ListenerTLSConfig),
      timer = timer,
      nettyTimer = nettyTimer,
      statsReceiver = statsReceiver,
      monitor = monitor,
      logger = logger
    )

    val channelMaxIdleTime = config.hostConnectionMaxIdleTime getOrElse Duration.Top
    val channelMaxLifeTime = config.hostConnectionMaxLifeTime getOrElse Duration.Top
    val serverDispatcher =
      if (channelMaxIdleTime < Duration.Top || channelMaxLifeTime < Duration.Top) {
        val idleTime = if (channelMaxIdleTime < Duration.Top) Some(channelMaxIdleTime) else None
        val lifeTime = if (channelMaxLifeTime < Duration.Top) Some(channelMaxLifeTime) else None
        ExpiringServerDispatcher[Req, Rep](
          idleTime, lifeTime, timer,
          statsReceiver.scope("expired"),
          codec.newServerDispatcher)
      } else  codec.newServerDispatcher _

    val server = DefaultServer[Req, Rep, Rep, Req](
      name = config.name,
      listener = listener,
      serviceTransport = serverDispatcher,
      requestTimeout = config.requestTimeout getOrElse Duration.Top,
      maxConcurrentRequests = config.maxConcurrentRequests getOrElse Int.MaxValue,
      cancelOnHangup = config.cancelOnHangup,
      prepare = codec.prepareConnFactory(_),
      timer = timer,
      monitor = monitor,
      logger = logger,
      statsReceiver = statsReceiver,
      tracer = tracer,
      reporter = NullReporterFactory
    )

    val factory = config.openConnectionsThresholds map { (threshold) =>
      new IdleConnectionFilter(serviceFactory, threshold, statsReceiver.scope("idle"))
    } getOrElse serviceFactory

    val listeningServer = server.serve(config.bindTo, factory)
    val closed = new AtomicBoolean(false)

    if (!config.daemon) ExitGuard.guard()
    def close(deadline: Time): Future[Unit] = {
      if (!closed.compareAndSet(false, true)) {
        logger.log(Level.WARNING, "Server closed multiple times!",
          new Exception/*stack trace please*/)
        return Future.exception(new IllegalStateException)
      }

      listeningServer.close(deadline) ensure {
        if (!config.daemon) ExitGuard.unguard()
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
  def unsafeBuild(service: Service[Req, Rep]): Server =
    withConfig(_.validated).build(service)
}
