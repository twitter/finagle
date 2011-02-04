package com.twitter.finagle.builder

import scala.collection.JavaConversions._

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
import com.twitter.util.{Future, Promise, Return}
import service.{StatsFilter, ExpiringService, TimeoutFilter}
import stats.{StatsReceiver}

trait Server {
  def close(): Future[Void]
}

object ServerBuilder {
  def apply() = new ServerBuilder[Any, Any]()
  def get() = apply()

  lazy val defaultChannelFactory =
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

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = codec.serverPipelineFactory.getPipeline

        for (maxConcurrentRequests <- _maxConcurrentRequests) {
          val maxQueueDepth = _maxQueueDepth.getOrElse(Int.MaxValue)
          val queue = new LinkedBlockingQueue[Job](maxQueueDepth)
          pipeline.addFirst("queue", new QueueingChannelHandler(maxConcurrentRequests, queue))
        }

        for (logger <- _logger) {
          pipeline.addFirst(
            "channelLogger", ChannelSnooper(_name getOrElse "server")(logger.info))
        }

        // SSL comes first so that ChannelSnooper gets plaintext
        for (ctx <- _tls) {
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(false)
          sslEngine.setEnableSessionCreation(true)
          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        var service = codec.wrapServerChannel(serviceFactory())
        if (_statsReceiver.isDefined)
          service = new StatsFilter(_statsReceiver.get).andThen(service)

        // We add the idle time after the codec. This ensures that a
        // client couldn't DoS us by sending lots of little messages
        // that don't produce a request object for some time. In other
        // words, the idle time refers to the idle time from the view
        // of the protocol.
        _hostConnectionMaxIdleTime foreach { duration =>
          val closingHandler = new ChannelClosingHandler
          pipeline.addLast("closingHandler", closingHandler)

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
        
        // Active request set? 


        pipeline.addLast("service", new ServiceToChannelHandler(service))
        pipeline
      }
    })

    val channel = bs.bind(_bindTo.get)
    new Server {
      def close() = {
        val done = new Promise[Void]
        channel.close() { case _ => done() = Return(null) }

        // Wait for outstanding requests.

        // XXX cf.releaseExternalResources()
        done
      }
    }
  }
}
