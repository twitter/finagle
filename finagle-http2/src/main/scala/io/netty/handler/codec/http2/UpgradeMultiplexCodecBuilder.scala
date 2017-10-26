// we use the netty package because otherwise we can't extend
// Http2MultiplexCodecBuilder.
package io.netty.handler.codec.http2

import io.netty.channel._

private class UpgradeMultiplexCodecBuilder(
    handler: ChannelHandler)
  extends Http2MultiplexCodecBuilder(true, handler) {

  override def build(
    decoder: Http2ConnectionDecoder,
    encoder: Http2ConnectionEncoder,
    initialSettings: Http2Settings
  ): Http2MultiplexCodec =
    new Http2MultiplexCodec(encoder, decoder, initialSettings, childHandler) {
      // TODO: rip this out when https://github.com/netty/netty/issues/7280 gets fixed
      override def onBytesConsumed(
        ctx: ChannelHandlerContext,
        stream: Http2FrameStream,
        bytes: Int
      ): Unit = {
        // opt out of flow control for the h2c upgrade--it's actually an http/1.1 message
        if (stream.id() != 1)
          consumeBytes(stream.id(), bytes)
      }
    }
}

object UpgradeMultiplexCodecBuilder {
  def forServer(handler: ChannelHandler): Http2MultiplexCodecBuilder =
    new UpgradeMultiplexCodecBuilder(handler)
}
