package com.twitter.finagle.builder

import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.logging.Logger
import java.net.SocketAddress
import javax.net.ssl.SSLContext

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.ReadTimeoutHandler

import com.twitter.util.{Time, Duration}
import com.twitter.conversions.time._

import com.twitter.finagle._
import com.twitter.finagle.channel.WriteCompletionTimeoutHandler
import com.twitter.finagle.tracing.{TraceReceiver, TracingFilter, NullTraceReceiver}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._
import com.twitter.finagle.util.Timer._
import com.twitter.util.{Future, Promise, Return, Throw}

import channel.{ChannelClosingHandler, ServiceToChannelHandler, ChannelSemaphoreHandler}
import service.{ExpiringService, TimeoutFilter, StatsFilter}
import stats.StatsReceiver

trait Server {
  /**
   * Close the underlying server gracefully with the given grace
   * period. close() will drain the current channels, waiting up to
   * ``timeout'', after which channels are forcibly closed.
   */
  def close(timeout: Duration = Duration.MaxValue)
}

object ServerBuilder {
  def apply() = new ServerBuilder[Any, Any]()
  def get() = apply()

  val defaultChannelFactory =
    new ReferenceCountedChannelFactory(
      new LazyRevivableChannelFactory(() =>
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool())))
}

// TODO: common superclass between client & server builders for common
// concerns.

case class ServerBuilder[Req, Rep](
  _codec: Option[Codec[Req, Rep]],
  _statsReceiver: Option[StatsReceiver],
  _name: Option[String],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _bindTo: Option[SocketAddress],
  _logger: Option[Logger],
  _tls: Option[(String, String)],
  _startTls: Boolean,
  _channelFactory: Option[ReferenceCountedChannelFactory],
  _maxConcurrentRequests: Option[Int],
  _hostConnectionMaxIdleTime: Option[Duration],
  _requestTimeout: Option[Duration],
  _readTimeout: Option[Duration],
  _writeCompletionTimeout: Option[Duration],
  _traceReceiver: TraceReceiver)
{
  import ServerBuilder._

  def this() = this(
    None,                  // codec
    None,                  // statsReceiver
    None,                  // name
    None,                  // sendBufferSize
    None,                  // recvBufferSize
    None,                  // bindTo
    None,                  // logger
    None,                  // tls
    false,                 // startTls
    None,                  // channelFactory
    None,                  // maxConcurrentRequests
    None,                  // hostConnectionMaxIdleTime
    None,                  // requestTimeout
    None,                  // readTimeout
    None,                  // writeCompletionTimeout
    new NullTraceReceiver  // traceReceiver
  )

  private[this] def options = Seq(
    "codec"                     -> _codec,
    "statsReceiver"             -> _statsReceiver,
    "name"                      -> _name,
    "sendBufferSize"            -> _sendBufferSize,
    "recvBufferSize"            -> _recvBufferSize,
    "bindTo"                    -> _bindTo,
    "logger"                    -> _logger,
    "tls"                       -> _tls,
    "startTls"                  -> Some(_startTls),
    "channelFactory"            -> _channelFactory,
    "maxConcurrentRequests"     -> _maxConcurrentRequests,
    "hostConnectionMaxIdleTime" -> _hostConnectionMaxIdleTime,
    "requestTimeout"            -> _requestTimeout,
    "readTimeout"               -> _readTimeout,
    "writeCompletionTimeout"    -> _writeCompletionTimeout,
    "traceReceiver"             -> Some(_traceReceiver)
  )

  override def toString() = {
    "ServerBuilder(%s)".format(
      options flatMap {
        case (k, Some(v)) => Some("%s=%s".format(k, v))
        case _ => None
      } mkString(", "))
  }

  def codec[Req1, Rep1](codec: Codec[Req1, Rep1]) =
    copy(_codec = Some(codec))

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  def name(value: String) = copy(_name = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def bindTo(address: SocketAddress) =
    copy(_bindTo = Some(address))

  def channelFactory(cf: ReferenceCountedChannelFactory) =
    copy(_channelFactory = Some(cf))

  def logger(logger: Logger) = copy(_logger = Some(logger))

  def tls(certificatePath: String, keyPath: String) =
    copy(_tls = Some((certificatePath, keyPath)))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def maxConcurrentRequests(max: Int) =
    copy(_maxConcurrentRequests = Some(max))

  def hostConnectionMaxIdleTime(howlong: Duration) =
    copy(_hostConnectionMaxIdleTime = Some(howlong))

  def requestTimeout(howlong: Duration) =
    copy(_requestTimeout = Some(howlong))

  def readTimeout(howlong: Duration) =
    copy(_readTimeout = Some(howlong))

  def writeCompletionTimeout(howlong: Duration) =
    copy(_writeCompletionTimeout = Some(howlong))

  def traceReceiver(receiver: TraceReceiver) =
    copy(_traceReceiver = receiver)

  private[this] def scopedStatsReceiver =
    _statsReceiver map { sr => _name map (sr.scope(_)) getOrElse sr }

  def build(service: Service[Req, Rep]): Server = build(() => service)

  def build(serviceFactory: () => Service[Req, Rep]): Server = {
    val codec = _codec.getOrElse {
      throw new IncompleteSpecification("No codec was specified")
    }

    val cf = _channelFactory getOrElse defaultChannelFactory
    cf.acquire()
    val bs = new ServerBootstrap(new ChannelFactoryToServerChannelFactory(cf))

    bs.setOption("tcpNoDelay", true)
    // bs.setOption("soLinger", 0) // XXX: (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }

    // TODO: we need something akin to a max queue depth.
    val queueingChannelHandlerAndGauges =
      _maxConcurrentRequests map { maxConcurrentRequests =>
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

    val queueingChannelHandler =
      queueingChannelHandlerAndGauges map { case (q, _) => q }
    val gauges =
      queueingChannelHandlerAndGauges.toList flatMap { case (_, g) => g }

    trait ChannelHandle {
      def drain(): Future[Unit]
      def close()
    }

    val channels = new HashSet[ChannelHandle]

    // Share this stats receiver to avoid per-connection overhead.
    val statsFilter = scopedStatsReceiver map { new StatsFilter[Req, Rep](_) }

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = codec.serverPipelineFactory.getPipeline

        _logger foreach { logger =>
          pipeline.addFirst(
            "channelLogger", ChannelSnooper(_name getOrElse "server")(logger.info))
        }

        // XXX/TODO: add stats for both read & write completion
        // timeouts.

        // Note that the timeout is *after* request decoding. This
        // prevents death from clients trying to DoS by slowly
        // trickling in bytes to our (accumulating) codec.
        _readTimeout foreach { howlong =>
          val (timeoutValue, timeoutUnit) = howlong.inTimeUnit
          pipeline.addLast(
            "readTimeout",
            new ReadTimeoutHandler(Timer.defaultNettyTimer, timeoutValue, timeoutUnit))
        }

        _writeCompletionTimeout foreach { howlong =>
          pipeline.addLast(
            "writeCompletionTimeout",
            new WriteCompletionTimeoutHandler(Timer.default, howlong))
        }

        // SSL comes first so that ChannelSnooper gets plaintext
        _tls foreach { case (certificatePath, keyPath) =>
          val sslEngine = Ssl.server(certificatePath, keyPath).createSSLEngine()
          sslEngine.setUseClientMode(false)
          sslEngine.setEnableSessionCreation(true)

          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        // Serialization keeps the codecs honest.
        pipeline.addLast(
          "requestSerializing",
          new ChannelSemaphoreHandler(new AsyncSemaphore(1)))

        // Add the (shared) queueing handler *after* request
        // serialization as it assumes one outstanding request per
        // channel.
        queueingChannelHandler foreach { pipeline.addLast("queue", _) }

        // Compose the service stack.
        var service = codec.wrapServerChannel(serviceFactory())

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
        _hostConnectionMaxIdleTime foreach { duration =>
          service = new ExpiringService(service, duration) {
            override def didExpire() { closingHandler.close() }
          }
        }

        _requestTimeout foreach { duration =>
          service = (new TimeoutFilter(duration)) andThen service
        }

        // This has to go last (ie. first in the stack) so that
        // protocol-specific trace support can override our generic
        // one here.
        service = (new TracingFilter(_traceReceiver)) andThen service

        // Register the channel so we can wait for them for a
        // drain. We close the socket but wait for all handlers to
        // complete (to drain them individually.)  Note: this would be
        // complicated by the presence of pipelining.
        val channelHandler = new ServiceToChannelHandler(service)

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

    val serverChannel = bs.bind(_bindTo.get)
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

      override def toString = {
        "Server(%s)".format(
          options flatMap {
            case (k, Some(v)) => Some("%s=%s".format(k, v))
            case _ => None
          } mkString(", "))
      }
    }
  }
}
