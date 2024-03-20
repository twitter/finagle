package com.twitter.finagle.memcached.compressing

import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.Lz4
import com.twitter.finagle.memcached.compressing.scheme.Lz4MemcachedCompression
import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression
import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression.FlagsAndBuf
import com.twitter.finagle.memcached.compressing.scheme.Uncompressed
import com.twitter.finagle.memcached.compressing.scheme.UncompressedMemcachedCompression
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Try
import com.twitter.io.Buf

case class CompressionProvider(
  compressionScheme: CompressionScheme,
  statsReceiver: StatsReceiver) {

  private final val MemcachedCompressionByScheme: Map[CompressionScheme, MemcachedCompression] =
    Map(
      Uncompressed -> UncompressedMemcachedCompression,
      Lz4 -> Lz4MemcachedCompression(statsReceiver)
    )

  val compressor: Buf => FlagsAndBuf = {
    MemcachedCompressionByScheme(compressionScheme).apply
  }

  val decompressor: FlagsAndBuf => Try[Buf] = flagsAndBuf => {
    val (flags, _) = flagsAndBuf
    val compressionScheme = CompressionScheme.compressionType(flags)
    Try(MemcachedCompressionByScheme(compressionScheme).invert(flagsAndBuf))
  }
}
