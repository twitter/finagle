package com.twitter.finagle.builder
/**
 * Provides a class for building servers.
 * The main class to use is [[com.twitter.finagle.builder.ServerBuilder]], as so
 * {{{
 * ServerBuilder()
 *   .codec(Http)
 *   .hostConnectionMaxLifeTime(5.minutes)
 *   .readTimeout(2.minutes)
 *   .build(plusOneService)
 * }}}
 */

import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.Executors
import java.util.logging.Logger
import java.net.SocketAddress
import javax.net.ssl.{SSLContext, SSLEngine}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.ReadTimeoutHandler

import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.finagle._
import com.twitter.finagle.channel.{
  WriteCompletionTimeoutHandler, ChannelStatsHandler,
  ChannelRequestStatsHandler}
import com.twitter.finagle.tracing.{TraceReceiver, TracingFilter, NullTraceReceiver}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._
import com.twitter.finagle.util.Timer._
import com.twitter.util.Future
import com.twitter.concurrent.AsyncSemaphore

import channel.{ChannelClosingHandler, ServiceToChannelHandler, ChannelSemaphoreHandler}
import service.{ExpiringService, TimeoutFilter, StatsFilter, ProxyService}
import stats.{StatsReceiver, NullStatsReceiver}

trait Server {
  /**
   * Close the underlying server gracefully with the given grace
   * period. close() will drain the current channels, waiting up to
   * ``timeout'', after which channels are forcibly closed.
   */
  def close(timeout: Duration = Duration.MaxValue)
}

/**
 * Factory for [[com.twitter.finagle.builder.ServerBuilder]] instances
 */
object ServerBuilder {
  def apply() = new ServerBuilder[Any, Any]
  def get() = apply()

  val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool())))
}

/**
 * A configuration object that represents what shall be built.
 */
final case class ServerConfig[Req, Rep](
  private val _codec:                     Option[ServerCodec[Req, Rep]]    = None,
  private val _statsReceiver:             Option[StatsReceiver]            = None,
  private val _name:                      Option[String]                   = None,
  private val _sendBufferSize:            Option[Int]                      = None,
  private val _recvBufferSize:            Option[Int]                      = None,
  private val _bindTo:                    Option[SocketAddress]            = None,
  private val _logger:                    Option[Logger]                   = None,
  private val _tls:                       Option[(String, String)]         = None,
  private val _startTls:                  Boolean                          = false,
  private val _channelFactory:            ReferenceCountedChannelFactory   = ServerBuilder.defaultChannelFactory,
  private val _maxConcurrentRequests:     Option[Int]                      = None,
  private val _hostConnectionMaxIdleTime: Option[Duration]                 = None,
  private val _hostConnectionMaxLifeTime: Option[Duration]                 = None,
  private val _requestTimeout:            Option[Duration]                 = None,
  private val _readTimeout:               Option[Duration]                 = None,
  private val _writeCompletionTimeout:    Option[Duration]                 = None,
  private val _traceReceiver:             TraceReceiver                    = new NullTraceReceiver)
{
  /**
   * The Scala compiler errors if the case class members don't have underscores.
   * Nevertheless, we want a friendly public API so we create delegators without
   * underscores.
   */
  val codec                     = _codec
  val statsReceiver             = _statsReceiver
  val name                      = _name
  val sendBufferSize            = _sendBufferSize
  val recvBufferSize            = _recvBufferSize
  val bindTo                    = _bindTo
  val logger                    = _logger
  val tls                       = _tls
  val startTls                  = _startTls
  val channelFactory            = _channelFactory
  val maxConcurrentRequests     = _maxConcurrentRequests
  val hostConnectionMaxIdleTime = _hostConnectionMaxIdleTime
  val hostConnectionMaxLifeTime = _hostConnectionMaxIdleTime
  val requestTimeout            = _requestTimeout
  val readTimeout               = _readTimeout
  val writeCompletionTimeout    = _writeCompletionTimeout
  val traceReceiver             = _traceReceiver

  def toMap = Map(
    "codec"                     -> _codec,
    "statsReceiver"             -> _statsReceiver,
    "name"                      -> _name,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "bindTo"                    -> _bindTo,
    "logger"                    -> _logger,
    "tls"                       -> _tls,
    "startTls"                  -> Some(_startTls),
    "channelFactory"            -> Some(_channelFactory),
    "maxConcurrentRequests"     -> _maxConcurrentRequests,
    "hostConnectionMaxIdleTime" -> _hostConnectionMaxIdleTime,
    "hostConnectionMaxLifeTime" -> _hostConnectionMaxLifeTime,
    "requestTimeout"            -> _requestTimeout,
    "readTimeout"               -> _readTimeout,
    "writeCompletionTimeout"    -> _writeCompletionTimeout,
    "traceReceiver"             -> Some(_traceReceiver)
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

  def assertValid() {
    _codec.getOrElse {
      throw new IncompleteSpecification("No codec was specified")
    }
    _bindTo.getOrElse {
      throw new IncompleteSpecification("No port was specified")
    }
  }
}

/**
 * A handy Builder for constructing Servers (i.e., binding Services to
 * a port).  This class is subclassable. Override copy() and build()
 * to do your own dirty work.
 */
class ServerBuilder[Req, Rep](val config: ServerConfig[Req, Rep]) {
  import ServerBuilder._

  def this() = this(new ServerConfig)

  override def toString() = "ServerBuilder(%s)".format(config.toString)

  protected def copy[Req1, Rep1](config: ServerConfig[Req1, Rep1]) =
    new ServerBuilder(config)

  protected def withConfig[Req1, Rep1](f: ServerConfig[Req, Rep] => ServerConfig[Req1, Rep1]) =
    copy(f(config))

  def codec[Req1, Rep1](codec: Codec[Req1, Rep1]) =
    withConfig(_.copy(_codec = Some(codec.serverCodec)))

  def codec[Req1, Rep1](codec: ServerCodec[Req1, Rep1]) =
    withConfig(_.copy(_codec = Some(codec)))

  def reportTo(receiver: StatsReceiver) =
    withConfig(_.copy(_statsReceiver = Some(receiver)))

  def name(value: String) =
    withConfig(_.copy(_name = Some(value)))

  def sendBufferSize(value: Int) =
    withConfig(_.copy(_sendBufferSize = Some(value)))

  def recvBufferSize(value: Int) =
    withConfig(_.copy(_recvBufferSize = Some(value)))

  def bindTo(address: SocketAddress) =
    withConfig(_.copy(_bindTo = Some(address)))

  def channelFactory(cf: ReferenceCountedChannelFactory) =
    withConfig(_.copy(_channelFactory = cf))

  def logger(logger: Logger) =
    withConfig(_.copy(_logger = Some(logger)))

  def tls(certificatePath: String, keyPath: String) =
    withConfig(_.copy(_tls = Some(certificatePath, keyPath)))

  def startTls(value: Boolean) =
    withConfig(_.copy(_startTls = true))

  def maxConcurrentRequests(max: Int) =
    withConfig(_.copy(_maxConcurrentRequests = Some(max)))

  def hostConnectionMaxIdleTime(howlong: Duration) =
    withConfig(_.copy(_hostConnectionMaxIdleTime = Some(howlong)))

  def hostConnectionMaxLifeTime(howlong: Duration) =
    withConfig(_.copy(_hostConnectionMaxLifeTime = Some(howlong)))

  def requestTimeout(howlong: Duration) =
    withConfig(_.copy(_requestTimeout = Some(howlong)))

  def readTimeout(howlong: Duration) =
    withConfig(_.copy(_readTimeout = Some(howlong)))

  def writeCompletionTimeout(howlong: Duration) =
    withConfig(_.copy(_writeCompletionTimeout = Some(howlong)))

  def traceReceiver(receiver: TraceReceiver) =
    withConfig(_.copy(_traceReceiver = receiver))

  /**
   * Construct the Server, given the provided Service.
   */
  def build(service: Service[Req, Rep]): Server = build(() => service)

  /**
   * Construct the Server, given the provided ServiceFactory. This
   * is useful if the protocol is stateful (e.g., requires authentication
   * or supports transactions).
   */
  def build(serviceFactory: () => Service[Req, Rep]): Server = {
    config.assertValid()

    val scopedStatsReceiver =
      config.statsReceiver map { sr => config.name map (sr.scope(_)) getOrElse sr }

    val codec = config.codec.get

    val cf = config.channelFactory
    cf.acquire()
    val bs = new ServerBootstrap(new ChannelFactoryToServerChannelFactory(cf))

    bs.setOption("tcpNoDelay", true)
    // bs.setOption("soLinger", 0) // XXX: (TODO)
    bs.setOption("reuseAddress", true)
    config.sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    config.recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }

    // TODO: we need something akin to a max queue depth.
    val queueingChannelHandlerAndGauges =
      config.maxConcurrentRequests map { maxConcurrentRequests =>
        val semaphore = new AsyncSemaphore(maxConcurrentRequests)
        val gauges = scopedStatsReceiver.toList flatMap { sr =>
          sr.addGauge("request_concurrency") {
            maxConcurrentRequests - semaphore.numPermitsAvailable
          } :: sr.addGauge("request_queue_size") {
            semaphore.numWaiters
          } :: Nil
        }

        (new ChannelSemaphoreHandler(semaphore), gauges)
      }

    val queueingChannelHandler = queueingChannelHandlerAndGauges map { case (q, _) => q }
    val gauges = queueingChannelHandlerAndGauges.toList flatMap { case (_, g) => g }

    trait ChannelHandle {
      def drain(): Future[Unit]
      def close()
    }

    val channels = new HashSet[ChannelHandle]

    // We share some filters & handlers for cumulative stats.
    val statsFilter                = scopedStatsReceiver map { new StatsFilter[Req, Rep](_) }
    val channelStatsHandler        = scopedStatsReceiver map { new ChannelStatsHandler(_) }
    val channelRequestStatsHandler = scopedStatsReceiver map { new ChannelRequestStatsHandler(_) }

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = codec.pipelineFactory.getPipeline

        config.logger foreach { logger =>
          pipeline.addFirst(
            "channelLogger", ChannelSnooper(config.name getOrElse "server")(logger.info))
        }

        channelStatsHandler foreach { handler =>
          pipeline.addFirst("channelStatsHandler", handler)
        }

        // XXX/TODO: add stats for both read & write completion
        // timeouts.

        // Note that the timeout is *after* request decoding. This
        // prevents death from clients trying to DoS by slowly
        // trickling in bytes to our (accumulating) codec.
        config.readTimeout foreach { howlong =>
          val (timeoutValue, timeoutUnit) = howlong.inTimeUnit
          pipeline.addLast(
            "readTimeout",
            new ReadTimeoutHandler(Timer.defaultNettyTimer, timeoutValue, timeoutUnit))
        }

        config.writeCompletionTimeout foreach { howlong =>
          pipeline.addLast(
            "writeCompletionTimeout",
            new WriteCompletionTimeoutHandler(Timer.default, howlong))
        }

        // SSL comes first so that ChannelSnooper gets plaintext
        config.tls foreach { case (certificatePath, keyPath) =>
          val ctx = Ssl.server(certificatePath, keyPath)
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(false)
          sslEngine.setEnableSessionCreation(true)

          pipeline.addFirst("ssl", new SslHandler(sslEngine, config.startTls))
        }

        // Serialization keeps the codecs honest.
        pipeline.addLast("requestSerializing", new ChannelSemaphoreHandler(new AsyncSemaphore(1)))

        // Add this after the serialization to get an accurate request
        // count.
        channelRequestStatsHandler foreach { handler =>
          pipeline.addLast("channelRequestStatsHandler", handler)
        }

        // Add the (shared) queueing handler *after* request
        // serialization as it assumes at most one outstanding request
        // per channel.
        queueingChannelHandler foreach { pipeline.addLast("queue", _) }

        // Compose the service stack.
        var service: Service[Req, Rep] = {
          val underlying = serviceFactory()
          val prepared   = codec.prepareService(underlying)
          new ProxyService(prepared)
        }

        statsFilter foreach { sf =>
          service = sf andThen service
        }

        // We add the idle time after the codec. This ensures that a
        // client couldn't DoS us by sending lots of little messages
        // that don't produce a request object for some time. In other
        // words, the idle time refers to the idle time from the view
        // of the protocol.

        // TODO: can we share closing handler instances with the
        // channelHandler?

        val closingHandler = new ChannelClosingHandler
        pipeline.addLast("closingHandler", closingHandler)

        if (config.hostConnectionMaxIdleTime.isDefined ||
            config.hostConnectionMaxLifeTime.isDefined) {
          service =
            new ExpiringService(
              service,
              config.hostConnectionMaxIdleTime,
              config.hostConnectionMaxLifeTime
            ) {
              override def didExpire() { closingHandler.close() }
            }
        }

        config.requestTimeout foreach { duration =>
          service = (new TimeoutFilter(duration)) andThen service
        }

        // This has to go last (ie. first in the stack) so that
        // protocol-specific trace support can override our generic
        // one here.
        service = (new TracingFilter(config.traceReceiver)) andThen service

        // Register the channel so we can wait for them for a
        // drain. We close the socket but wait for all handlers to
        // complete (to drain them individually.)  Note: this would be
        // complicated by the presence of pipelining.
        val channelHandler = new ServiceToChannelHandler(
          service, scopedStatsReceiver getOrElse NullStatsReceiver)

        val handle = new ChannelHandle {
          def close() =
            channelHandler.close()
          def drain() = {
            channelHandler.drain()
            channelHandler.onShutdown
          }
        }

        channels.synchronized { channels += handle }
        channelHandler.onShutdown ensure {
          channels.synchronized {
            channels.remove(handle)
          }
        }

        pipeline.addLast("channelHandler", channelHandler)
        pipeline
      }
    })

    val serverChannel = bs.bind(config.bindTo.get)
    Timer.default.acquire()
    new Server {
      def close(timeout: Duration = Duration.MaxValue) = {
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
        val joined = Future.join(channels.synchronized { channels toArray } map { _.drain() })

        // Wait for all channels to shut down.
        joined.get(timeout)

        // Force close any remaining connections. Don't wait for
        // success. Buffer channels into an array to avoid
        // deadlocking.
        channels.synchronized { channels toArray } foreach { _.close() }

        // Release any gauges we've created.
        gauges foreach { _.remove() }

        bs.releaseExternalResources()
        Timer.default.stop()
      }

      override def toString = "Server(%s)".format(config.toString)
    }
  }
}
