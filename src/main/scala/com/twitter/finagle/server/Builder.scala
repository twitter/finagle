package com.twitter.finagle.server

import java.net.InetSocketAddress
import java.util.concurrent.{TimeUnit, Executors}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel.socket.nio._

import com.twitter.finagle._
import com.twitter.finagle.util._
import com.twitter.finagle.thrift._

import com.twitter.ostrich
import com.twitter.util.TimeConversions._
import com.twitter.util.Duration

object Http extends Codec {
  val pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpServerCodec)
        pipeline
      }
    }
}

object Thrift extends Codec {
  val pipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftCodec", new ThriftServerCodec)
        pipeline
      }
    }
}

object Codec {
  val http = Http
  val thrift = Thrift
}

object Builder {
  def apply() = new Builder()
  def get() = apply()

  val channelFactory =
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

}

case class Builder(
  _port: Int,
  _codec: Option[Codec],
  _connectionTimeout: Timeout,
  _requestTimeout: Timeout,
  _statsReceiver: Option[StatsReceiver],
  _sampleWindow: Timeout,
  _sampleGranularity: Timeout,
  _name: Option[String],
  _sendBufferSize: Option[Int],
  _recvBufferSize: Option[Int])
{
  import Builder._

  def this() = this(
    0,                                              // port (default, ephemeral)
    None,                                           // codec
    Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),  // connectionTimeout
    Timeout(Long.MaxValue, TimeUnit.MILLISECONDS),  // requestTimeout
    None,                                           // statsReceiver
    Timeout(10, TimeUnit.MINUTES),                  // sampleWindow
    Timeout(10, TimeUnit.SECONDS),                  // sampleGranularity
    None,                                           // name
    None,                                           // sendBufferSize
    None                                            // recvBufferSize
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

  def build() = {
    if (!_codec.isDefined) throw new IncompleteConfiguration("No codec was specified")

    val bs = new ServerBootstrap(channelFactory)
    bs.setOption("tcpNoDelay", true) // XXX: right?
    // bs.setOption("soLinger", 0) // XXX: (TODO)
    bs.setOption("reuseAddress", true)
    _sendBufferSize foreach { s =>  bs.setOption("sendBufferSize", s) }
    _recvBufferSize foreach { s => bs.setOption("receiveBufferSize", s) }

    val sampleRepository = new SampleRepository

    // Construct sample stats.
    val granularity = _sampleGranularity.duration
    val window      = _sampleWindow.duration
    if (window < granularity) {
      throw new IncompleteConfiguration(
        "window smaller than granularity!")
    }
    val numBuckets = math.max(1, window.inMilliseconds / granularity.inMilliseconds)
    val statsMaker = () => new TimeWindowedSample[ScalarSample](numBuckets.toInt, granularity)
    val namePrefix = _name map ("%s_".format(_)) getOrElse ""

//    new LoadBalancedBroker(statsBrokers)
  }

  // def buildClient[Request, Reply]() =
  //   new Server[HttpRequest, HttpResponse](build())

}

class Server(bootstrap: ServerBootstrap) {
}

