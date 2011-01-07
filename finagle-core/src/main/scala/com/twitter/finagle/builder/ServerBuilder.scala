package com.twitter.finagle.builder

import scala.collection.JavaConversions._

import java.net.SocketAddress
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.logging.Logger
import javax.net.ssl.{KeyManager, SSLContext}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.ssl._
import org.jboss.netty.channel.socket.nio._

import com.twitter.ostrich
import com.twitter.util.TimeConversions._
import com.twitter.util.{Duration, Time}

import com.twitter.finagle._
import channel.{Job, QueueingChannelHandler}
import com.twitter.finagle.util._
import com.twitter.finagle.service.{Service, ServicePipelineFactory}
import org.jboss.netty.util.HashedWheelTimer

object ServerBuilder {
  def apply() = new ServerBuilder()
  def get() = apply()

  val defaultChannelFactory =
    new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())
}

class SampleHandler(samples: SampleRepository[AddableSample[_]])
  extends SimpleChannelHandler{
  val dispatchSample: AddableSample[_] = samples("dispatch")
  val latencySample: AddableSample[_]  = samples("latency")

  case class Timing(requestedAt: Time = Time.now)

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    ctx.getAttachment match {
      case Timing(requestedAt: Time) =>
        samples("exception", e.getCause.getClass.getName).add(
          requestedAt.untilNow.inMilliseconds.toInt)
      case _ => ()
    }
    super.exceptionCaught(ctx, e)
  }

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (c.isInstanceOf[MessageEvent]) {
      dispatchSample.incr()
      ctx.setAttachment(Timing())
    }

    super.handleUpstream(ctx, c)
  }

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (c.isInstanceOf[MessageEvent]) {
      val e = c.asInstanceOf[MessageEvent]
      (ctx.getAttachment, e.getMessage) match {
        case (Timing(requestedAt: Time), r: HttpResponse)  =>
          latencySample.add(requestedAt.untilNow.inMilliseconds.toInt)
        case (_, _) =>
          () // WTF?
      }
    }

    super.handleDownstream(ctx, c)
  }
}

// TODO: common superclass between client & server builders for common
// concerns.

case class ServerBuilder(
  _codec: Option[Codec],
  _statsReceiver: Option[StatsReceiver],
  _sampleWindow: Duration,
  _sampleGranularity: Duration,
  _name: Option[String],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int],
  _pipelineFactory: Option[ChannelPipelineFactory],
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
    None,        // codec
    None,        // statsReceiver
    10.minutes,  // sampleWindow
    10.seconds,  // sampleGranularity
    None,        // name
    None,        // sendBufferSize
    None,        // recvBufferSize
    None,        // pipelineFactory
    None,        // bindTo
    None,        // logger
    None,        // tls
    false,       // startTls
    None,        // channelFactory
    None,        // maxConcurrentRequests
    None         // maxQueueDepth
  )

  def codec(codec: Codec): ServerBuilder =
    copy(_codec = Some(codec))

  def reportTo(receiver: StatsReceiver): ServerBuilder =
    copy(_statsReceiver = Some(receiver))

  def sampleWindow(window: Duration): ServerBuilder =
    copy(_sampleWindow = window)

  def sampleGranularity(window: Duration): ServerBuilder =
    copy(_sampleGranularity = window)

  def name(value: String): ServerBuilder = copy(_name = Some(value))

  def sendBufferSize(value: Int): ServerBuilder = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int): ServerBuilder = copy(_recvBufferSize = Some(value))

  def pipelineFactory(value: ChannelPipelineFactory): ServerBuilder =
    copy(_pipelineFactory = Some(value))

  def service[Req <: AnyRef, Rep <: AnyRef](service: Service[Req, Rep]): ServerBuilder =
    copy(_pipelineFactory = Some(ServicePipelineFactory(service)))

  def bindTo(address: SocketAddress): ServerBuilder =
    copy(_bindTo = Some(address))

  def channelFactory(cf: ChannelFactory): ServerBuilder =
    copy(_channelFactory = Some(cf))

  def logger(logger: Logger): ServerBuilder = copy(_logger = Some(logger))

  def tls(path: String, password: String): ServerBuilder =
    copy(_tls = Some(Ssl(path, password)))

  def startTls(value: Boolean): ServerBuilder =
    copy(_startTls = true)

  def maxConcurrentRequests(max: Int): ServerBuilder =
    copy(_maxConcurrentRequests = Some(max))

  def maxQueueDepth(max: Int): ServerBuilder =
    copy(_maxQueueDepth = Some(max))

  private def statsRepository(
    name: Option[String],
    receiver: Option[StatsReceiver],
    window: Duration,
    granularity: Duration,
    sockAddr: SocketAddress) =
  {
    if (window < granularity) {
      throw new IncompleteSpecification(
        "window smaller than granularity!")
    }

    // .

    val prefix = name map ("%s_".format(_)) getOrElse ""
    val sampleRepository =
      new ObservableSampleRepository[TimeWindowedSample[ScalarSample]] {
        override def makeStat = TimeWindowedSample[ScalarSample](window, granularity)
      }

    for (receiver <- receiver)
      sampleRepository observeTailsWith receiver.observer(prefix, sockAddr toString)

    sampleRepository
  }

  def build(): Channel = {
    val (codec, pipelineFactory) = (_codec, _pipelineFactory) match {
      case (None, _) =>
        throw new IncompleteSpecification("No codec was specified")
      case (_, None) =>
        throw new IncompleteSpecification("No pipeline was specified")
      case (Some(codec), Some(pipeline)) =>
        (codec, pipeline)
    }

   val bs = new ServerBootstrap(_channelFactory getOrElse defaultChannelFactory)

    bs.setOption("tcpNoDelay", true)
    // bs.setOption("soLinger", 0) // XXX: (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s => bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }

    val statsRepo = statsRepository(
      _name, _statsReceiver,
      _sampleWindow, _sampleGranularity,
      _bindTo.get)

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

        pipeline.addLast("stats", new SampleHandler(statsRepo))

        for ((name, handler) <- pipelineFactory.getPipeline.toMap)
          pipeline.addLast(name, handler)

        pipeline
      }
    })

    bs.bind(_bindTo.get)
  }
}
