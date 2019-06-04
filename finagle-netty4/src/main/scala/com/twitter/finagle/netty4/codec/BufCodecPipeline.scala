package com.twitter.finagle.netty4.codec

import io.netty.channel.ChannelPipeline

private[finagle] object BufCodecPipeline extends (ChannelPipeline => Unit) {
  def apply(pipeline: ChannelPipeline): Unit =
    pipeline.addLast(BufCodec.Key, BufCodec)
}
