package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.transport.Netty4Framer
import com.twitter.finagle.netty4.CopyingByteReaderDecoder
import com.twitter.finagle.netty4.codec.BufCodec
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._

private[finagle] object PipelineInit extends Netty4Framer {

  def bufferManagerName: String = "muxPipelineInit"

  def MessageDecoderName = "ByteBufToByteReader"

  def BufEncoderName = "BufToByteBufEncoder"

  // Note: this is sharable by default.
  val bufferManager: ChannelHandler = new ChannelInitializer[Channel] {
    override def initChannel(ch: Channel): Unit = {
      val pipeline = ch.pipeline()

      pipeline.addLast(MessageDecoderName, CopyingByteReaderDecoder)
      pipeline.addLast(BufEncoderName, OutboundBufEncoder)
    }
  }

  @Sharable
  private[this] object OutboundBufEncoder extends ChannelOutboundHandlerAdapter {
    override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit =
      BufCodec.write(ctx, msg, promise)
  }
}
