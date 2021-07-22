package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack
import com.twitter.finagle.decoder.Decoder
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.decoder.DecoderHandler
import io.netty.channel._

private[netty4] object Netty4ClientChannelInitializer {
  val DecoderKey = "decoder"
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
 * @param decoderFactory initialize per-channel decoder for emitting messages.
 */
final private[netty4] class Netty4ClientChannelInitializer[T](
  params: Stack.Params,
  decoderFactory: Option[() => Decoder[T]] = None)
    extends AbstractNetty4ClientChannelInitializer(params) {
  import Netty4ClientChannelInitializer._

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)

    // fist => last
    // - a request flies from last to first
    // - a response flies from first to last
    //
    // [pipeline from super.initChannel] => bufCodec => decoder

    val pipe = ch.pipeline

    pipe.addLast(BufCodec.Key, BufCodec)

    decoderFactory.foreach { newDecoder =>
      pipe.addLast(DecoderKey, new DecoderHandler(newDecoder()))
    }
  }
}
