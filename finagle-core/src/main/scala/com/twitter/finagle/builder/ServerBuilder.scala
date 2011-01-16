package com.twitter.finagle.builder

import scala.collection.JavaConversions._

import java.net.SocketAddress
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.logging.Logger
import javax.net.ssl.SSLContext

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.channel.socket.nio._

import com.twitter.util.TimeConversions._

import com.twitter.finagle._
import channel.{Job, QueueingChannelHandler}
import com.twitter.finagle.util._
import service.{StatsFilter, ServiceToChannelHandler, Service}
import stats.{StatsReceiver}

object ServerBuilder {
  def apply() = new ServerBuilder()
  def get() = apply()

  val defaultChannelFactory =
    new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())
}

// TODO: common superclass between client & server builders for common
// concerns.

case class ServerBuilder[Req <: AnyRef, Res <: AnyRef](
  _codec: Option[Codec],
  _statsReceiver: Option[StatsReceiver],
  _name: Option[String],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _service: Option[Service[Req, Res]],
  _bindTo: Option[SocketAddress],
  _logger: Option[Logger],
  _tls: Option[SSLContext],
  _startTls: Boolean,
  _channelFactory: Option[ChannelFactory],
  _maxConcurrentRequests: Option[Int],
  _maxQueueDepth: Option[Int])
{
  import ServerBuilder._

  def this() = this(
    None,              // codec
    None,              // statsReceiver
    None,              // name
    None,              // sendBufferSize
    None,              // recvBufferSize
    None,              // service
    None,              // bindTo
    None,              // logger
    None,              // tls
    false,             // startTls
    None,              // channelFactory
    None,              // maxConcurrentRequests
    None               // maxQueueDepth
  )

  def codec(codec: Codec) =
    copy(_codec = Some(codec))

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  def name(value: String) = copy(_name = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def service[Req <: AnyRef, Rep <: AnyRef](service: Service[Req, Rep]) =
    copy(_service = Some(service))

  def bindTo(address: SocketAddress) =
    copy(_bindTo = Some(address))

  def channelFactory(cf: ChannelFactory) =
    copy(_channelFactory = Some(cf))

  def logger(logger: Logger) = copy(_logger = Some(logger))

  def tls(path: String, password: String) =
    copy(_tls = Some(Ssl.server(path, password)))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def maxConcurrentRequests(max: Int) =
    copy(_maxConcurrentRequests = Some(max))

  def maxQueueDepth(max: Int) =
    copy(_maxQueueDepth = Some(max))

  def build(): Channel = {
    val codec = _codec.getOrElse {
      throw new IncompleteSpecification("No codec was specified")
    }

   val bs = new ServerBootstrap(_channelFactory getOrElse defaultChannelFactory)

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

          // val mon = new Thread {
          //   while (true) {
          //     println("   Engine: %s".format(sslEngine))
          //     println("Handshake: %s".format(sslEngine.getHandshakeStatus))
          //     println()
          //     Thread.`yield`()
          //     Thread.sleep(1000)
          //   }
          // } // .run()


          pipeline.addFirst("ssl", new SslHandler(sslEngine, _startTls))
        }

        _service.foreach { service =>
          val serviceWithStats =
            if (_statsReceiver.isDefined)
              new StatsFilter(_statsReceiver.get).andThen(service)
            else service
          pipeline.addLast("service", new ServiceToChannelHandler(serviceWithStats))
        }

        pipeline
      }
    })

    bs.bind(_bindTo.get)
  }
}
