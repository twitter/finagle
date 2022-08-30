package com.twitter.finagle.netty4.codec.compression.zstd;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

public class ZstdStreamingEncoder extends ChannelOutboundHandlerAdapter {

  private final int level;
  private ZstdByteBufDirectBufferCompressingStream compressor;

  public ZstdStreamingEncoder(int level) {
    this.level = level;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise channelPromise)
    throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf cast = (ByteBuf) msg;
      try {
        compressor.compressByteBuf(cast);
        // TODO: this breaks backpressure as the bytes are not really sent but since we only use
        //  it in (thrift)mux which doesn't need TCP level backpressure it's fine for now.
        channelPromise.setSuccess();
      } finally {
        ReferenceCountUtil.release(cast);
      }
    } else {
      super.write(ctx, msg, channelPromise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    compressor.flush();
    ctx.flush();
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    compressor = new ZstdByteBufDirectBufferCompressingStream(ctx, level);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    closeCompressor();
  }

  private void closeCompressor() throws IOException {
    if (compressor != null) {
      compressor.close();
      compressor = null;
    }
  }
}
