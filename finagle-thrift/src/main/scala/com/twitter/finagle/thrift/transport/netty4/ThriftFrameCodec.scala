package com.twitter.finagle.thrift.transport.netty4

import io.netty.channel.{ChannelHandler, CombinedChannelDuplexHandler}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

/**
 * An implementation of a Thrift framer using netty4 primitives.
 */
private[netty4] object ThriftFrameCodec {
  private val maxFrameLength = Integer.MAX_VALUE // 0x7FFFFFFF
  private val lengthFieldOffset = 0
  private val lengthFieldLength = 4
  private val lengthAdjustment = 0
  private val initialBytesToStrip = 4

  def apply(): ChannelHandler = {
    val encoder = new LengthFieldPrepender(ThriftFrameCodec.lengthFieldLength)
    val decoder = new LengthFieldBasedFrameDecoder(
      ThriftFrameCodec.maxFrameLength,
      ThriftFrameCodec.lengthFieldOffset,
      ThriftFrameCodec.lengthFieldLength,
      ThriftFrameCodec.lengthAdjustment,
      ThriftFrameCodec.initialBytesToStrip
    )

    new CombinedChannelDuplexHandler(decoder, encoder)
  }
}
