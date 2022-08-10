package com.twitter.finagle.netty4.codec.compression.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * This is an experimental encoder for ZStd which uses a streaming
 * compressor implementation.
 * We will hopefully upstream this to Netty in the future once we're
 * convinced it works well.
 */
public class ZstdStreamingEncoder extends ChannelOutboundHandlerAdapter {

  private ZstdByteBufDirectBufferCompressingStream compressor;
  private final ByteBuffer inputBuf =
      ByteBuffer.allocateDirect(ZstdDirectBufferCompressingStream.recommendedOutputBufferSize());
  private final ByteBuffer outputBuf =
      ByteBuffer.allocateDirect(ZstdDirectBufferCompressingStream.recommendedOutputBufferSize());

  private final int level;

  public ZstdStreamingEncoder(int level) {
    this.level = level;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise channelPromise)
    throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf cast = (ByteBuf) msg;
      try {
        compressor.compress(cast);
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
  public void close(final ChannelHandlerContext ctx, final ChannelPromise promise)
      throws Exception {
    closeCompressor();
    ctx.close(promise);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    inputBuf.limit(inputBuf.position());
    outputBuf.limit(outputBuf.capacity());
    compressor = new ZstdByteBufDirectBufferCompressingStream(ctx, inputBuf, outputBuf, level);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    closeCompressor();
    super.handlerRemoved(ctx);
  }

  private void closeCompressor() throws IOException {
    if (compressor != null) {
      compressor.close();
      compressor = null;
    }

    inputBuf.clear();
    outputBuf.clear();
  }
}
