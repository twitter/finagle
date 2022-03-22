package com.twitter.finagle.netty4.codec.compression.zstd;

import com.github.luben.zstd.ZstdDirectBufferDecompressingStream;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

/**
 * Specialization of the ZstdDirectBufferDecompressingStream which supports a single streaming buffer
 * for decompression
 */
class ZstdRefillableDirectBufferDecompressingStream
    extends ZstdDirectBufferDecompressingStream {

  private ByteBuffer source;

  /**
   * Constructor to initialize underlying decompression wrapper and retain handle to buffer
   * @param source ByteBuffer that will hold and cycle through input data
   */
  public ZstdRefillableDirectBufferDecompressingStream(ByteBuffer source) {
    super(source);

    this.source = source;
  }

  /**
   * Siphon bytes from an input ByteBuf to the decompression stream buffer
   * The super class has an overridable "refill" method that is called on source exhaustion,
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
  public void transferBuffer(ByteBuf in) {
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
