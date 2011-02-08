package com.twitter.finagle.builder

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

import java.net.SocketAddress
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.logging.Logger
import javax.net.ssl.SSLContext

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.handler.ssl._
import org.jboss.netty.channel.socket.nio._

import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.finagle._
import com.twitter.finagle.util.Conversions._
import channel.{Job, QueueingChannelHandler, ChannelClosingHandler, ServiceToChannelHandler}
import com.twitter.finagle.util._
import com.twitter.finagle.util.Timer._
import com.twitter.util.{Future, Promise, Return, Throw}
import service.{StatsFilter, ExpiringService, TimeoutFilter}
import stats.{StatsReceiver}

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
  _tls: Option[SSLContext],
  _startTls: Boolean,
  _channelFactory: Option[ReferenceCountedChannelFactory],
  _maxConcurrentRequests: Option[Int],
  _hostConnectionMaxIdleTime: Option[Duration],
  _requestTimeout: Option[Duration],
  _maxQueueDepth: Option[Int])
{
  import ServerBuilder._

  def this() = this(
    None,              // codec
    None,              // statsReceiver
    None,              // name
    None,              // sendBufferSize
    None,              // recvBufferSize
    None,              // bindTo
    None,              // logger
    None,              // tls
    false,             // startTls
    None,              // channelFactory
    None,              // maxConcurrentRequests
    None,              // hostConnectionMaxIdleTime
    None,              // requestTimeout
    None               // maxQueueDepth
  )

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

  def tls(path: String, password: String) =
    copy(_tls = Some(Ssl.server(path, password)))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def maxConcurrentRequests(max: Int) =
    copy(_maxConcurrentRequests = Some(max))

  def hostConnectionMaxIdleTime(howlong: Duration) =
    copy(_hostConnectionMaxIdleTime = Some(howlong))

  def requestTimeout(howlong: Duration) =
    copy(_requestTimeout = Some(howlong))

  def maxQueueDepth(max: Int) =
    copy(_maxQueueDepth = Some(max))

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
     
    val queueingChannelHandler = _maxConcurrentRequests map { maxConcurrentRequests =>
      val maxQueueDepth = _maxQueueDepth.getOrElse(Int.MaxValue)
      val queue = new LinkedBlockingQueue[Job](maxQueueDepth)
      new QueueingChannelHandler(maxConcurrentRequests, queue)
    }

    trait ChannelHandle {
      def drain(): Future[Unit]
      def close()
    }

    val channels = new HashSet[ChannelHandle]

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = codec.serverPipelineFactory.getPipeline

        queueingChannelHandler foreach { pipeline.addFirst("queue", _) }

        _logger foreach { logger =>
          pipeline.addFirst(
            "channelLogger", ChannelSnooper(_name getOrElse "server")(logger.info))
        }

        // SSL comes first so that ChannelSnooper gets plaintext
        _tls foreach { ctx =>
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(false)
          sslEngine.setEnableSessionCreation(true)
          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        // Compose the service stack.
        var service = codec.wrapServerChannel(serviceFactory())

        _statsReceiver foreach { statsReceiver =>
          service = (new StatsFilter(statsReceiver)) andThen service
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

        // Register the channel so we can wait for them for a
        // drain. We close the socket but wait for all handlers to
        // complete (to drain them individually.)  Note: this would be
        // complicated by the presence of pipelining.

        // serialize requests?

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
        val joined = channels.synchronized {
          Future.join(channels.toSeq map { _.drain() })
        }

        // Wait for all channels to shut down.
        joined.get(timeout)

        // Force close any remaining connections. Don't wait for
        // success. Buffer channels into an array to avoid
        // deadlocking.
        channels.synchronized { channels toArray } foreach { _.close() }

        bs.releaseExternalResources()
        Timer.default.stop()
      }
    }
  }
}
