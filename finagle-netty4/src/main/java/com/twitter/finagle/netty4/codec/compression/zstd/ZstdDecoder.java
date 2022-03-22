package com.twitter.finagle.netty4.codec.compression.zstd;

import com.github.luben.zstd.ZstdDirectBufferDecompressingStream;

import com.twitter.finagle.netty4.codec.compression.MalformedInputException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.twitter.finagle.netty4.codec.compression.zstd.ZstdConstants.MAX_BLOCK_SIZE;
import static com.twitter.finagle.netty4.codec.compression.zstd.ZstdConstants.SIZE_OF_MAGIC;

/**
 * A streaming decoder that decompresses Zstd compressed data. It "manually" parses out the frame
 * header in order to allocate appropriate output chunks and a buffer to hold the required lookback
 * window, but then delegates to a JNI-based library to handle the streaming decompression
 */
public class ZstdDecoder extends ByteToMessageDecoder {

  private static final int ZSTD_MAGIC = 0xfd2fb528;
  private static final int V_07_ZSTD_MAGIC = 0xfd2fb527;

  private enum ZstdDecoderState {
    CheckMagic,
    DecompressData,
    Corrupted
  }

  private ZstdDecoderState decompressionState = ZstdDecoderState.CheckMagic;
  private ZstdRefillableDirectBufferDecompressingStream decompresser = null;
  private int decoded = 0;
  private final ByteBuffer inputBuf = ByteBuffer.allocateDirect(MAX_BLOCK_SIZE);
  private final ByteBuffer outputBuf =
    ByteBuffer.allocateDirect(ZstdDirectBufferDecompressingStream.recommendedTargetBufferSize());

  public ZstdDecoder() {
    inputBuf.limit(inputBuf.position());
    outputBuf.limit(outputBuf.capacity());
  }

  @Override
  public void decode(
    ChannelHandlerContext ctx,
    ByteBuf in,
    List<Object> out
  ) throws Exception {
    while (in.isReadable()) {
      try {
        switch (decompressionState) {
          case CheckMagic:
            if (in.readableBytes() >= SIZE_OF_MAGIC) {
              verifyMagic(in);

              if (decompresser == null) {
                decompresser = new ZstdRefillableDirectBufferDecompressingStream(inputBuf);
              }

              decompressionState = ZstdDecoderState.DecompressData;
            } else {
              // wait for more bytes
              return;
            }
            break;
          case DecompressData:
            try {
              // has side effects on `in`, `out`, and `decompressor`
              consumeAndDecompress(ctx, in, out);

              if (!decompresser.hasRemaining()) {
                resetState(true);
              }
            } catch (Exception e) {
                resetState(false);
                throw e;
            }
            break;
          case Corrupted:
            in.skipBytes(in.readableBytes());
            break;
          default:
            throw new AssertionError();
        }
      } catch (Exception e) {
          decompressionState = ZstdDecoderState.Corrupted;
          throw e;
      }
    }
  }

  @Override
  protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
    resetState(false);
  }

  private void consumeAndDecompress(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws IOException {
    do {
      decompresser.transferBuffer(in);
      decoded = decompresser.read(outputBuf);
      if (decoded > 0) {
        outputBuf.flip();
        out.add(ctx.alloc().buffer(decoded, decoded).writeBytes(outputBuf));
        outputBuf.position(0);
      }
    } while (decoded > 0);
  }

  private void resetState(boolean retainBuf) throws IOException {
    // done! reset decoder state and clear buffers for next frame
    decompresser.close();
    decompresser = null;
    if (retainBuf) {
      // retain the buffer that we have not yet read
      // e.g. we have bytes that span two frames pending (should be rare)
      inputBuf.compact();
      inputBuf.limit(inputBuf.position());
      inputBuf.position(0);
    } else {
      inputBuf.clear();
    }

    decompressionState = ZstdDecoderState.CheckMagic;
  }

  static void verifyMagic(ByteBuf buf) throws MalformedInputException {
    int magic = buf.getIntLE(buf.readerIndex());
    if (magic != ZSTD_MAGIC) {
      if (magic == V_07_ZSTD_MAGIC) {
        throw new MalformedInputException(
            buf.readerIndex(),
            "Data encoded in unsupported ZSTD v0.7 format");
      }
      throw new MalformedInputException(
          buf.readerIndex(),
          "Invalid magic prefix: " + Integer.toHexString(magic));
    }
  }
}
