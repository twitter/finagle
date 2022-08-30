package com.twitter.finagle.netty4.codec.compression.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class ZstdByteBufDirectBufferCompressingStream extends ZstdDirectBufferCompressingStream {

  private final ChannelHandlerContext ctx;
  private ByteBuf currentByteBuf;


  // This is 131591 by default, just over 128 KB.
  private static final int DEFAULT_BUFFER_SIZE =
      ZstdDirectBufferCompressingStream.recommendedOutputBufferSize();

  // This value is currently is 16448, or ~16 KB.
  private static final int BUFFER_COPY_THRESHOLD = DEFAULT_BUFFER_SIZE  / 8;

  private static ByteBuffer byteBufferView(ByteBuf buffer) {
    // We perform a `.slice()` operation to ensure that our view of the buffer is pure:
    // the index will start at 0 and the size will be DEFAULT_BUFFER_SIZE.
    return buffer.internalNioBuffer(buffer.writerIndex(), DEFAULT_BUFFER_SIZE).slice();
  }

  private static ByteBuf allocateByteBuf(ChannelHandlerContext ctx) {
    return ctx.alloc().directBuffer(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Constructor to initialize underlying decompression wrapper and retain handle to buffer
   */
  public ZstdByteBufDirectBufferCompressingStream(ChannelHandlerContext ctx, int level
  ) throws IOException {
    this(ctx, allocateByteBuf(ctx), level);
  }

  private ZstdByteBufDirectBufferCompressingStream(ChannelHandlerContext ctx,
                                                   ByteBuf initial, int level) throws IOException {
    super(byteBufferView(initial), level);
    this.ctx = ctx;
    currentByteBuf = initial;
  }

  /**
   * compression method used by the encoder
   * @param in to compress
   * @throws IOException
   */
  public void compressByteBuf(ByteBuf in) throws IOException {
    if (in.isDirect() && in.isContiguous()) {
      compressDirect(in);
    } else {
      compressHeap(in);
    }
  }

  private void compressHeap(ByteBuf in) throws IOException {
    ByteBuf newDirectIn = ctx.alloc().directBuffer(in.readableBytes());
    try {
      // Advances the reader index of `in` automatically.
      newDirectIn.writeBytes(in);
      compressDirect(newDirectIn);
    } finally {
      newDirectIn.release();
    }
  }

  private void compressDirect(ByteBuf in) throws IOException {
    final int readableBytes = in.readableBytes();
    ByteBuffer byteBuffer = in.internalNioBuffer(in.readerIndex(), readableBytes);

    while (byteBuffer.hasRemaining()) {
        compress(byteBuffer);
    }
    // Make sure we advance our buffer.
    in.readerIndex(in.readerIndex() + readableBytes);
  }

  // This is the method that `ZstdDirectBufferCompressingStream` uses to tell the application
  // that the buffer needs to be flushed to the destination (at some point) and expects to receive
  // a buffer, which can be new or the same but with space available, to store more bytes.
  @Override
  protected ByteBuffer flushBuffer(ByteBuffer toFlush) {
    // `toFlush` is the underlying `ByteBuffer` that backs `currentByteBuf` and they have the same
    // size because we did a `.slice()` call when getting our view.
    final int bytesWritten = DEFAULT_BUFFER_SIZE - toFlush.remaining();

    // Decide if we want to copy the bytes into a new smaller buffer or just send the whole
    // thing and reallocate a new buffer.
    final ByteBuf out;
    if (bytesWritten < BUFFER_COPY_THRESHOLD) {
      // Copy the bytes into a new smaller buffer and keep on trucking.
      out = ctx.alloc().directBuffer(bytesWritten);
      // This variant of `writeBytes` does not mutate the indexes of `currentByteBuf`
      out.writeBytes(currentByteBuf, currentByteBuf.writerIndex(), bytesWritten);
    } else {
      // We're going to send off `currentByteBuf` and allocate a new one, but first we need
      // to bump netty's view of the data to reflect what was written to the underlying buffer.
      currentByteBuf.writerIndex(currentByteBuf.writerIndex() + bytesWritten);
      out = currentByteBuf;
      currentByteBuf = allocateByteBuf(ctx);
    }

    ctx.write(out, ctx.voidPromise());
    return byteBufferView(currentByteBuf);
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (currentByteBuf != null) {
      currentByteBuf.release();
      currentByteBuf = null;
    }
  }
}
