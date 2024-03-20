package com.twitter.finagle.memcached.compressing.scheme

import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression.FlagsAndBuf
import com.twitter.io.Buf

object UncompressedMemcachedCompression extends MemcachedCompression {

  override val compressionScheme: CompressionScheme = Uncompressed

  override protected def compress(buf: Buf): FlagsAndBuf =
    (Uncompressed.compressionFlags, buf)

  override protected def decompress(flagsAndBuf: FlagsAndBuf): Buf = {
    val (_, buf) = flagsAndBuf
    buf
  }

}
