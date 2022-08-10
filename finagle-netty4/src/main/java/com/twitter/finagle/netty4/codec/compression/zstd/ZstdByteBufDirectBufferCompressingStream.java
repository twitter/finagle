package com.twitter.finagle.netty4.codec.compression.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * This is an experimental streaming compressor for ZStd
 * We will hopefully upstream this to Netty in the future once we're
 * convinced it works well.
 */
public class ZstdByteBufDirectBufferCompressingStream extends ZstdDirectBufferCompressingStream {

  private final ByteBuffer source;
  private final ChannelHandlerContext ctx;

  /**
   * Constructor to initialize underlying decompression wrapper and retain handle to buffer
   * @param target ByteBuffer that will hold and cycle through input data
   */
  public ZstdByteBufDirectBufferCompressingStream(ChannelHandlerContext ctx,
      ByteBuffer source, ByteBuffer target, int level
  ) throws IOException {
    super(target, level);
    this.ctx = ctx;
    this.source = source;
  }

  /**
   * compression method used by the encoder
   * @param in to compress
   * @throws IOException
   */
  public void compress(ByteBuf in) throws IOException {
    while (in.isReadable()) {
      transferBuffer(in);
      compress(source);
    }
  }

  @Override
  protected ByteBuffer flushBuffer(ByteBuffer toFlush) {
    toFlush.flip();
    int toWrite = toFlush.remaining();
    if (toWrite > 0) {
      ByteBuf out = ctx.alloc().buffer(toWrite);
      out.writeBytes(toFlush);
      ctx.write(out);
    }

    toFlush.clear();
    return toFlush;
  }

  /**
   * Siphon bytes from an input ByteBuf to the decompression stream buffer
   * The super class has an overridable "refill" method that is called on target exhaustion,
   * but this is much friendlier to the Netty system wherein we can't control the sizes of
   * incoming bytes, and we don't have block header info to know how many bytes to buffer per
   * block.
   *
   * This manually swaps the buffer to "write mode" to append the bytes, then flips
   * back to "read" mode for the bytes that have yet to be decompressed.
   *
   * @param in input ByteBuf, ostensibly from a Channel
   * @return true if we dropped off some bytes
   */
  private void transferBuffer(ByteBuf in) {
    if (source.limit() + in.readableBytes() > source.capacity()) {
      source.compact();
      source.flip();
    }
    int safeReadable = Math.min(in.readableBytes(), source.capacity() - source.limit());
    if (safeReadable > 0) {
      source.mark();
      source.position(source.limit());
      source.limit(source.limit() + safeReadable);
      in.readBytes(source);
      source.reset();
    }
  }
}
