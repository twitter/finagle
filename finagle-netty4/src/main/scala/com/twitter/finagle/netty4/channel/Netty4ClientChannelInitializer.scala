package com.twitter.finagle.netty4.channel

import com.twitter.finagle.framer.Framer
import com.twitter.finagle.netty4.AnyToHeapInboundHandlerName
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.framer.FrameHandler
import com.twitter.finagle.Stack
import io.netty.channel._

private[netty4] object Netty4ClientChannelInitializer {
  val BufCodecKey = "bufCodec"
  val FramerKey = "framer"
  val WriteTimeoutHandlerKey = "writeTimeout"
  val ReadTimeoutHandlerKey = "readTimeout"
  val ConnectionHandlerKey = "connectionHandler"
  val ChannelStatsHandlerKey = "channelStats"
  val ChannelRequestStatsHandlerKey = "channelRequestStats"
  val ChannelLoggerHandlerKey = "channelLogger"
}

/**
 * Client channel initialization logic.
 *
 * @param params configuration parameters.
 * @param framerFactory initialize per-channel framer for emitting framed
 *                      messages.
 */
private[netty4] class Netty4ClientChannelInitializer(
    params: Stack.Params,
    framerFactory: Option[() => Framer] = None)
  extends AbstractNetty4ClientChannelInitializer(params) {
  import Netty4ClientChannelInitializer._

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)

    // fist => last
    // - a request flies from last to first
    // - a response flies from first to last
    //
    // [pipeline from super.initChannel] => bufCodec => framer

    val pipe = ch.pipeline

    pipe.addLast(AnyToHeapInboundHandlerName, AnyToHeapInboundHandler)
    pipe.addLast(BufCodecKey, BufCodec)

    framerFactory.foreach { newFramer =>
      pipe.addLast(FramerKey, new FrameHandler(newFramer()))
    }
  }
}
