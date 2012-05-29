package com.twitter.finagle.builder

import com.twitter.concurrent.{NamedPoolThreadFactory, AsyncSemaphore}
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.channel._
import com.twitter.finagle.filter.{
  HandletimeFilter, MonitorFilter, RequestSemaphoreFilter, MaskCancelFilter}
import com.twitter.finagle.service.{
  ExpiringService, ProxyService, StatsFilter, TimeoutFilter}
import com.twitter.finagle.ssl.{
  Engine, Ssl, SslIdentifierHandler, SslShutdownHandler}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.{Tracer, TracingFilter, NullTracer}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._
import com.twitter.util.{Future, Duration, Monitor, NullMonitor, Time, Timer, Promise}
import java.net.SocketAddress
import java.util.concurrent.Executors
import java.util.logging.{Logger, Level}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{ChannelGroupFuture,
  DefaultChannelGroup, DefaultChannelGroupFuture, ChannelGroupFutureListener}
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.ReadTimeoutHandler
import org.jboss.netty.{util => nu}
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashSet, SynchronizedSet}

trait Server {
  /**
   * Close the underlying server gracefully with the given grace
   * period. close() will drain the current channels, waiting up to
   * ``timeout'', after which channels are forcibly closed.
   */
  def close(timeout: Duration = Duration.MaxValue)

  /**
   * When a server is bound to an ephemeral port, gets back the address
   * with concrete listening port picked.
   */
  def localAddress: SocketAddress
}

trait BoundServer {
  def boundAddress: SocketAddress
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
    builder.build(service)

  val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleServerBoss")),
          Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleServerIO"))
        )
      )
    )
}

object ServerConfig {
  sealed abstract trait Yes
  type FullySpecified[Req, Rep] = ServerConfig[Req, Rep, Yes, Yes, Yes]
}

case class BufferSize(
  send: Option[Int] = None,
  recv: Option[Int] = None
)

case class TimeoutConfig(
  hostConnectionMaxIdleTime: Option[Duration] = None,
  hostConnectionMaxLifeTime: Option[Duration] = None,
  requestTimeout: Option[Duration] = None,
  readTimeout: Option[Duration] = None,
  writeCompletionTimeout: Option[Duration] = None
)

/**
 * A configuration object that represents what shall be built.
 */
final case class ServerConfig[Req, Rep, HasCodec, HasBindTo, HasName](
  private val _codecFactory:                    Option[CodecFactory[Req, Rep]#Server]    = None,
  private val _statsReceiver:                   Option[StatsReceiver]                    = None,
  private val _monitor:                         Option[(String, SocketAddress) => Monitor] = None,
  private val _name:                            Option[String]                           = None,
  private val _bufferSize:                      BufferSize                               = BufferSize(),
  private val _keepAlive:                       Option[Boolean]                          = None,
  private val _backlog:                         Option[Int]                              = None,
  private val _bindTo:                          Option[SocketAddress]                    = None,
  private val _logger:                          Option[Logger]                           = None,
  private val _tls:                             Option[(String, String, String, String, String)] = None,
  private val _channelFactory:                  ReferenceCountedChannelFactory           = ServerBuilder.defaultChannelFactory,
  private val _maxConcurrentRequests:           Option[Int]                              = None,
  private val _timeoutConfig:                   TimeoutConfig                            = TimeoutConfig(),
  private val _requestTimeout:                  Option[Duration]                         = None,
  private val _readTimeout:                     Option[Duration]                         = None,
  private val _writeCompletionTimeout:          Option[Duration]                         = None,
  private val _tracerFactory:                   Managed[Tracer]                          = Managed.const(NullTracer),
  private val _openConnectionsThresholds:       Option[OpenConnectionsThresholds]        = None,
  private val _serverBootstrap:                 Option[ServerBootstrap]                  = None,
  private val _cancelOnHangup:                  Boolean                                  = true)
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
  val tls                             = _tls
  val channelFactory                  = _channelFactory
  val maxConcurrentRequests           = _maxConcurrentRequests
  val hostConnectionMaxIdleTime       = _timeoutConfig.hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime       = _timeoutConfig.hostConnectionMaxLifeTime
  val requestTimeout                  = _timeoutConfig.requestTimeout
  val readTimeout                     = _timeoutConfig.readTimeout
  val writeCompletionTimeout          = _timeoutConfig.writeCompletionTimeout
  val timeoutConfig                   = _timeoutConfig
  val tracerFactory                   = _tracerFactory
  val openConnectionsThresholds       = _openConnectionsThresholds
  val serverBootstrap                 = _serverBootstrap
  val cancelOnHangup                  = _cancelOnHangup

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
    "tls"                             -> _tls,
    "channelFactory"                  -> Some(_channelFactory),
    "maxConcurrentRequests"           -> _maxConcurrentRequests,
    "hostConnectionMaxIdleTime"       -> _timeoutConfig.hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime"       -> _timeoutConfig.hostConnectionMaxLifeTime,
    "requestTimeout"                  -> _timeoutConfig.requestTimeout,
    "readTimeout"                     -> _timeoutConfig.readTimeout,
    "writeCompletionTimeout"          -> _timeoutConfig.writeCompletionTimeout,
    "tracerFactory"                   -> Some(_tracerFactory),
    "openConnectionsThresholds"       -> Some(_openConnectionsThresholds),
    "serverBootstrap"                 -> _serverBootstrap,
    "cancelOnHangup"                  -> Some(_cancelOnHangup)
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

  def channelFactory(cf: ReferenceCountedChannelFactory): This =
    withConfig(_.copy(_channelFactory = cf))

  def logger(logger: Logger): This =
    withConfig(_.copy(_logger = Some(logger)))

  def tls(certificatePath: String, keyPath: String,
          caCertificatePath: String = null, ciphers: String = null, nextProtos: String = null): This =
    withConfig(_.copy(_tls = Some(certificatePath, keyPath, caCertificatePath, ciphers, nextProtos)))

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

  def tracerFactory(factory: Tracer.Factory): This =
    withConfig(_.copy(_tracerFactory = Tracer.mkManaged(factory)))

  /**
   * Cancel pending futures whenever the the connection is shut down.
   * This defaults to true.
   */
  def cancelOnHangup(yesOrNo: Boolean): This =
    withConfig(_.copy(_cancelOnHangup = yesOrNo))

  /**
   * Used for testing.
   */
  private[builder] def serverBootstrap(bs: ServerBootstrap): This =
    withConfig(_.copy(_serverBootstrap = Some(bs)))

  def openConnectionsThresholds(thresholds: OpenConnectionsThresholds): This =
    withConfig(_.copy(_openConnectionsThresholds = Some(thresholds)))


  /* Builder methods follow */

  /**
   * Construct the Server, given the provided Service.
   */
  def build(service: Service[Req, Rep]) (
     implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
       ThisConfig =:= FullySpecifiedConfig
   ): Server = build(ServiceFactory.const(service))

  /**
   * Construct the Server, given the provided Service factory.
   */
  @deprecated("Use the ServiceFactory variant instead")
  def build(serviceFactory: () => Service[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build(_ => serviceFactory())

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  @deprecated("Use the ServiceFactory variant instead")
  def build(serviceFactory: (ClientConnection) => Service[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = build(new ServiceFactory[Req, Rep] {
    def apply(conn: ClientConnection) = Future.value(serviceFactory(conn))
    def close() = ()
  })

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  def build(serviceFactory: ServiceFactory[Req, Rep])(
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Server = {
    val managed = buildManaged(serviceFactory)
    val bound = managed.make()
    new Server {
      def close(timeout: Duration = Duration.MaxValue) = bound.dispose(timeout.fromNow).get()
      def localAddress = bound.get.boundAddress
    }
  }

  /**
   * Construct a Service, with runtime checks for builder
   * completeness.
   */
  def unsafeBuild(service: Service[Req, Rep]): Server =
    withConfig(_.validated).build(service)

  def buildManaged(serviceFactory: ServiceFactory[Req, Rep]) (
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Managed[BoundServer] = {
    val mkServer = new MkServer[Req, Rep](config, serviceFactory)
    mkServer()
  }

  def buildManaged(service: Service[Req, Rep]) (
    implicit THE_BUILDER_IS_NOT_FULLY_SPECIFIED_SEE_ServerBuilder_DOCUMENTATION:
      ThisConfig =:= FullySpecifiedConfig
  ): Managed[BoundServer] = buildManaged(ServiceFactory.const(service))
}

/**
 * MkServer builds a server from a configuration and service factory.
 */
private[builder] class MkServer[Req, Rep] (
  val config: ServerConfig.FullySpecified[Req, Rep],
  val inputServiceFactory: ServiceFactory[Req, Rep]
  ) {
  private[this] def bootstrap() = {
    config.channelFactory.acquire()
    val bs = config.serverBootstrap getOrElse {
      val serverCf = new ChannelFactoryToServerChannelFactory(config.channelFactory)
      new ServerBootstrap(serverCf)
    }

    bs.setOption("soLinger", 0) // XXX: (TODO)
    bs.setOption("reuseAddress", true)
    bs.setOption("child.tcpNoDelay", true)

    config.backlog.foreach { s => bs.setOption("backlog", s) }
    config.bufferSize.send foreach { s => bs.setOption("child.send", s) }
    config.bufferSize.recv foreach { s => bs.setOption("child.receiveBufferSize", s) }
    config.keepAlive.foreach { s => bs.setOption("child.keepAlive", s) }

    bs
  }

  private[this] def semaphoreFilter(
    statsReceiverOpt: Option[StatsReceiver],
    gauges: HashSet[stats.Gauge]
  ) = {
    val statsReceiver = statsReceiverOpt getOrElse NullStatsReceiver
    config.maxConcurrentRequests map { max =>
      val sem = new AsyncSemaphore(max)
      val g0 = statsReceiver.addGauge("request_concurrency") {
        max - sem.numPermitsAvailable
      }
      val g1 = statsReceiver.addGauge("request_queue_size") { sem.numWaiters }

      gauges += g0
      gauges += g1

      new RequestSemaphoreFilter[Req, Rep](sem)
    } getOrElse Filter.identity[Req, Rep]
  }

  private[this] def filter(
    timer: Timer,
    statsReceiverOpt: Option[StatsReceiver],
    gauges: HashSet[stats.Gauge]
  ) = {  // per connection filter stack
    val maskCancelFilter =
      if (config.cancelOnHangup) Filter.identity[Req, Rep]
      else new MaskCancelFilter[Req, Rep]

    val statsFilter = statsReceiverOpt map(new StatsFilter[Req, Rep](_)) getOrElse Filter.identity[Req, Rep]

    val timeoutFilter = config.requestTimeout map { duration =>
      val e = new IndividualRequestTimeoutException(duration)
      new TimeoutFilter[Req, Rep](duration, e, timer)
    } getOrElse Filter.identity[Req, Rep]

    maskCancelFilter andThen semaphoreFilter(statsReceiverOpt, gauges) andThen statsFilter andThen timeoutFilter
  }

  private[this] def serviceFactory(
    timer: Timer,
    statsReceiverOpt: Option[StatsReceiver],
    gauges: HashSet[stats.Gauge]
  ): ServiceFactory[Req, Rep] = {
    var factory = inputServiceFactory
    val statsReceiver = statsReceiverOpt getOrElse NullStatsReceiver

    config.openConnectionsThresholds foreach { threshold =>
      factory = new IdleConnectionFilter(
        factory, threshold, statsReceiver.scope("idle"))
    }

    filter(timer, statsReceiverOpt, gauges) andThen factory
  }

  private[this] def mkPipeline(
    codec: Codec[Req, Rep],
    statsReceiverOpt: Option[StatsReceiver],
    timer: Timer
  ) = {
    val pipeline = codec.pipelineFactory.getPipeline
    config.logger foreach { logger =>
      pipeline.addFirst(
        "channelLogger", ChannelSnooper(config.name)(logger.info))
    }

    // We share some filters & handlers for cumulative stats.
    val channelStatsHandler = statsReceiverOpt map { new ChannelStatsHandler(_) }
    val channelRequestStatsHandler = statsReceiverOpt map { new ChannelRequestStatsHandler(_) }

    channelStatsHandler foreach { handler =>
      pipeline.addFirst("channelStatsHandler", handler)
    }

    // Note that the timeout is *after* request decoding. This
    // prevents death from clients trying to DoS by slowly
    // trickling in bytes to our (accumulating) codec.
    config.readTimeout foreach { howlong =>
      val (timeoutValue, timeoutUnit) = howlong.inTimeUnit
      pipeline.addLast(
        "readTimeout",
        new ReadTimeoutHandler(new TimerToNettyTimer(timer), timeoutValue, timeoutUnit))
    }

    config.writeCompletionTimeout foreach { howlong =>
      pipeline.addLast(
        "writeCompletionTimeout",
        new WriteCompletionTimeoutHandler(timer, howlong))
    }

    // SSL comes first so that ChannelSnooper gets plaintext
    addTls(pipeline)

    // Add this after the serialization to get an accurate request
    // count.
    channelRequestStatsHandler foreach { handler =>
      pipeline.addLast("channelRequestStatsHandler", handler)
    }

    pipeline
  }

  private[this] def addTls(pipeline: ChannelPipeline) =
    config.tls foreach { case (certificatePath, keyPath, caCertificatePath, ciphers, nextProtos) =>
      val engine: Engine = Ssl.server(certificatePath, keyPath, caCertificatePath, ciphers, nextProtos)
      engine.self.setUseClientMode(false)
      engine.self.setEnableSessionCreation(true)

      val handler = new SslHandler(engine.self)

      // Certain engine implementations need to handle renegotiation internally,
      // as Netty's TLS protocol parser implementation confuses renegotiation and
      // notification events. Renegotiation will be enabled for those Engines with
      // a true handlesRenegotiation value.
      handler.setEnableRenegotiation(engine.handlesRenegotiation)

      pipeline.addFirst("ssl", handler)

      // Netty's SslHandler does not provide SSLEngine implementations any hints that they
      // are no longer needed (namely, upon disconnection.) Since some engine implementations
      // make use of objects that are not managed by the JVM's memory manager, we need to
      // know when memory can be released. The SslShutdownHandler will invoke the shutdown
      // method on implementations that define shutdown(): Unit.
      pipeline.addFirst(
        "sslShutdown",
        new SslShutdownHandler(engine)
      )

      // Information useful for debugging SSL issues, such as the certificate, cipher spec,
      // remote address is provided to the SSLEngine implementation by the SslIdentifierHandler.
      // The SslIdentifierHandler will invoke the setIdentifier method on implementations
      // that define setIdentifier(String): Unit.
      pipeline.addFirst(
        "sslIdentifier",
        new SslIdentifierHandler(engine, certificatePath, ciphers)
      )
    }

  private[this] def setPipelineFactory(
    codec: Codec[Req, Rep],
    bootstrap: ServerBootstrap,
    channels: DefaultChannelGroup,
    tracer: Tracer,
    timer: Timer,
    gauges: HashSet[stats.Gauge]
  ) {
    // Due to tracing semantics, we need to place these at the
    // very bottom of the service stack. We should reconsider
    // the tracing interface.
    val tracingFilter = new TracingFilter[Req, Rep](tracer)

    val monitor = config.monitor map(_(config.name, config.bindTo)) getOrElse NullMonitor

    // This method is called only once per build() so it's ok to call sr.scope -
    // it will be created only once
    val statsReceiverOpt = config.statsReceiver map { sr => sr.scope(config.name) }
    val statsReceiver = statsReceiverOpt getOrElse NullStatsReceiver

    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = mkPipeline(codec, statsReceiverOpt, timer)

        // Make some connection-specific changes to the service
        // factory.
        var thisServiceFactory = serviceFactory(timer, statsReceiverOpt, gauges)

        // We add the idle time after the codec. This ensures that a
        // client couldn't DoS us by sending lots of little messages
        // that don't produce a request object for some time. In other
        // words, the idle time refers to the idle time from the view
        // of the protocol.
        val idleTime = config.hostConnectionMaxIdleTime
        val lifeTime = config.hostConnectionMaxLifeTime

        if (idleTime.isDefined || lifeTime.isDefined) {
          val closingHandler = new ChannelClosingHandler
          pipeline.addLast("closingHandler", closingHandler)
          thisServiceFactory = thisServiceFactory map { service =>
            val closingService = new ServiceProxy(service) {
              override def release() {
                closingHandler.close()
                super.release()
              }
            }
            new ExpiringService(
              closingService, idleTime, lifeTime, timer,
              statsReceiver.scope("expired")
            )
          }
        }

        // These have to go last (ie. first in the stack) so that
        // protocol-specific trace support can override our generic
        // one here.
        val monitorFilter = new MonitorFilter[Req, Rep](
          monitor andThen Monitor.mk { case exc =>
            Logger.getLogger(config.name).log(Level.SEVERE, "A Service threw an exception", exc)
            false
          }
        )

        val handletimeFilter = new HandletimeFilter[Req, Rep](statsReceiver)

        val connServiceFactory =
          handletimeFilter andThen
          monitorFilter andThen
          tracingFilter andThen
          codec.prepareConnFactory(thisServiceFactory)

        val dispatcher = new ServiceDispatcher(
          connServiceFactory, codec.mkServerDispatcher, statsReceiver,
          Logger.getLogger(config.name), monitor, channels)
        pipeline.addLast("finagleDispatcher", dispatcher)

        pipeline
      }
    })
  }

  // Makes "unamanged" server instance, note lack of explicit lifetime management for dependencies.
  private[this] def mkDisposableServer(
    serverChannel: Channel,
    bootstrap: ServerBootstrap,
    channels: DefaultChannelGroup,
    gauges: HashSet[stats.Gauge],
    timer: Timer
  ) = new Disposable[BoundServer] {
    def dispose(deadline: Time) = {
      // According to NETTY-256, the following sequence of operations
      // has no race conditions.
      //
      //   - close the server socket  (awaitUninterruptibly)
      //   - close all open channels  (awaitUninterruptibly)
      //   - releaseExternalResources
      //
      // We modify this a little bit, to allow for graceful draining,
      // closing open channels only after the grace period.
      //
      // The next step here is to do a half-closed socket: we want to
      // suspend reading, but not writing to a socket.  This may be
      // important for protocols that do any pipelining, and may
      // queue in their codecs.

      // On cursory inspection of the relevant Netty code, this
      // should never block (it is little more than a close() syscall
      // on the FD).
      serverChannel.close().awaitUninterruptibly()

      // At this point, no new channels may be created.
      val active = channels.asScala
      for (ch <- active)
        ch.getPipeline.get(classOf[ServiceDispatcher[Req, Rep]]).drain()

      val closing = new DefaultChannelGroupFuture(
        channels, active map(_.getCloseFuture) asJava)

      val p = new Promise[Unit]
      closing.addListener(new ChannelGroupFutureListener {
        def operationComplete(f: ChannelGroupFuture) {
          p.setValue(())
        }
      })

      p.within(deadline - Time.now)(timer) ensure {
        channels.close()
        gauges foreach { _.remove() }
        // Force close any remaining connections. Don't wait for
        // success.
        bootstrap.releaseExternalResources()
      }
    }

    private[this] val _server = new BoundServer {
      def boundAddress: SocketAddress = serverChannel.getLocalAddress()
      override def toString = "Server(%s)".format(config.toString)
    }
    def get = _server
  }

  private[this] def mkManaged(
    tracer: Tracer,
    timer: Timer
  ) = new Managed[BoundServer] {
    def make() = {

      // TODO: gauges should be converted to managed resources.
      val gauges = new HashSet[stats.Gauge]
      val channels = new DefaultChannelGroup

      val serverConfig = ServerCodecConfig(serviceName = config.name, boundAddress = config.bindTo)
      val codec = config.codecFactory(serverConfig)
      val bs = bootstrap()
      setPipelineFactory(codec, bs, channels, tracer, timer, gauges)
      val serverChannel = bs.bind(config.bindTo)
      mkDisposableServer(serverChannel, bs, channels, gauges, timer)
    }
  }

  def apply(): Managed[BoundServer] = for {
      timer <- FinagleTimer.getManaged
      tracer <- config.tracerFactory
      server <- mkManaged(tracer, timer)
    } yield server

}
