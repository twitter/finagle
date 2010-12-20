package com.twitter.finagle.builder

import scala.collection.JavaConversions._

import java.net.SocketAddress
import java.util.concurrent.{TimeUnit, Executors, LinkedBlockingQueue}
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
import channel.{Job, QueueingChannelHandler, PartialUpstreamMessageEvent}
import com.twitter.finagle.util._
import com.twitter.finagle.thrift._
import com.twitter.finagle.service.{Service, ServicePipelineFactory}
import org.jboss.netty.handler.timeout.{ReadTimeoutHandler, WriteTimeoutHandler}
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
          requestedAt.ago.inMilliseconds.toInt)
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
        case (_, p: PartialUpstreamMessageEvent) =>
          ()
        case (Timing(requestedAt: Time), r: HttpResponse)  =>
          latencySample.add(requestedAt.ago.inMilliseconds.toInt)
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
  _connectionTimeout: Timeout,
  _requestTimeout: Timeout,
  _responseTimeout: Timeout,
  _statsReceiver: Option[StatsReceiver],
  _sampleWindow: Timeout,
  _sampleGranularity: Timeout,
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
    None,                                           // codec
    Timeout.Eternity,                               // connectionTimeout
    Timeout.Eternity,                               // requestTimeout
    Timeout.Eternity,                               // responseTimeout
    None,                                           // statsReceiver
    Timeout(10, TimeUnit.MINUTES),                  // sampleWindow
    Timeout(10, TimeUnit.SECONDS),                  // sampleGranularity
    None,                                           // name
    None,                                           // sendBufferSize
    None,                                           // recvBufferSize
    None,                                           // pipelineFactory
    None,                                           // bindTo
    None,                                           // logger
    None,                                           // tls
    false,                                          // startTls
    None,                                           // channelFactory
    None,                                           // maxConcurrentRequests
    None                                            // maxQueueDepth
  )

  def codec(codec: Codec) =
    copy(_codec = Some(codec))

  def connectionTimeout(value: Long, unit: TimeUnit) =
    copy(_connectionTimeout = Timeout(value, unit))

  def requestTimeout(value: Long, unit: TimeUnit) =
    copy(_requestTimeout = Timeout(value, unit))

  def reportTo(receiver: StatsReceiver) =
    copy(_statsReceiver = Some(receiver))

  def sampleWindow(value: Long, unit: TimeUnit) =
    copy(_sampleWindow = Timeout(value, unit))

  def sampleGranularity(value: Long, unit: TimeUnit) =
    copy(_sampleGranularity = Timeout(value, unit))

  def name(value: String) = copy(_name = Some(value))

  def sendBufferSize(value: Int) = copy(_sendBufferSize = Some(value))
  def recvBufferSize(value: Int) = copy(_recvBufferSize = Some(value))

  def pipelineFactory(value: ChannelPipelineFactory) =
    copy(_pipelineFactory = Some(value))

  def service[Req <: AnyRef, Rep <: AnyRef](service: Service[Req, Rep]) =
    copy(_pipelineFactory = Some(ServicePipelineFactory(service)))

  def bindTo(address: SocketAddress) =
    copy(_bindTo = Some(address))

  def channelFactory(cf: ChannelFactory) =
    copy(_channelFactory = Some(cf))

  def logger(logger: Logger) = copy(_logger = Some(logger))

  def tls(path: String, password: String) =
    copy(_tls = Some(Ssl(path, password)))

  def startTls(value: Boolean) =
    copy(_startTls = true)

  def maxConcurrentRequests(max: Int) =
    copy(_maxConcurrentRequests = Some(max))

  def maxQueueDepth(max: Int) =
    copy(_maxQueueDepth = Some(max))

  private def statsRepository(
    name: Option[String],
    receiver: Option[StatsReceiver],
    sampleWindow: Timeout,
    sampleGranularity: Timeout,
    sockAddr: SocketAddress) =
  {
    val window      = sampleWindow.duration
    val granularity = sampleGranularity.duration
    if (window < granularity) {
      throw new IncompleteSpecification(
        "window smaller than granularity!")
    }

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

        if (_requestTimeout != Some(Timeout.Eternity) || _responseTimeout != Some(Timeout.Eternity)) {
          val timer = new HashedWheelTimer

          if (_requestTimeout != Some(Timeout.Eternity)) {
            pipeline.addFirst("requestTimeout",
              new ReadTimeoutHandler(timer, _requestTimeout.value, _responseTimeout.unit))
          }

          if (_responseTimeout != Some(Timeout.Eternity)) {
            pipeline.addFirst("responseTimeout",
              new WriteTimeoutHandler(timer, _responseTimeout.value, _responseTimeout.unit))
          }
        }

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
