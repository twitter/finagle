package com.twitter.finagle.memcached.compressing.scheme

import com.twitter.io.Buf
import com.twitter.bijection.Bijection
import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression.FlagsAndBuf

object MemcachedCompression {
  type FlagsAndBuf = (Int, Buf)
}

trait MemcachedCompression extends Bijection[Buf, FlagsAndBuf] {

  val compressionScheme: CompressionScheme

  override final def apply(a: Buf): FlagsAndBuf = compress(a)

  override final def invert(b: FlagsAndBuf): Buf = {
    val (flags, _) = b
    val decompressCompressionScheme = CompressionScheme.compressionType(flags)
    if (decompressCompressionScheme == compressionScheme)
      decompress(b)
    else {
      // correct decompression function should always be chosen by [[com.twitter.finagle.memcached.compressing.CompressionProvider]]
      throw new IllegalStateException(
        s"Received $decompressCompressionScheme while $compressionScheme was expected for decompression")
    }
  }

  protected def compress(buf: Buf): FlagsAndBuf

  protected def decompress(flagsAndBuf: FlagsAndBuf): Buf

}
