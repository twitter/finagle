package com.twitter.finagle.memcached.compressing.scheme

import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression.FlagsAndBuf
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import java.nio.ByteBuffer
import net.jpountz.lz4.LZ4Factory

case class Lz4MemcachedCompression(statsReceiver: StatsReceiver) extends MemcachedCompression {

  override final val compressionScheme: CompressionScheme = Lz4

  // Factory for lz4-java compressors.
  private[this] final val Factory = LZ4Factory.fastestInstance()

  private[this] final val Compressor = Factory.fastCompressor()

  private[this] final val Decompressor = Factory.fastDecompressor()

  private[this] final val Stats = CompressingMemcachedClientStats(
    statsReceiver.scope(compressionScheme.name))

  // Only compress if the value is above certain bytes
  private[this] final val COMPRESS_ABOVE_BYTES = 50

  override protected def compress(buf: Buf): FlagsAndBuf = {
    val ba = Buf.ByteBuffer.Owned.extract(buf)

    // bytes to be compressed
    val remaining = ba.remaining()

    if (remaining > COMPRESS_ABOVE_BYTES) {
      compressedBufferAndFlags(remaining, buf, ba, Stats)
    } else {
      uncompressedBufferAndFlags(remaining, buf, Stats)
    }
  }

  private def compressedBufferAndFlags(
    remaining: Int,
    buf: Buf,
    byteBuffer: ByteBuffer,
    stats: CompressingMemcachedClientStats
  ): FlagsAndBuf = {
    val out = ByteBuffer.wrap(
      new Array[Byte](Compressor.maxCompressedLength(remaining))
    )
    Compressor.compress(byteBuffer, out)
    out.flip()
    stats.compressionAttemptedCounter.incr()

    // Frame includes the length of the uncompressed contents
    val lengthBuf = ByteBuffer.allocate(4)
    lengthBuf.putInt(remaining)
    lengthBuf.flip()

    // Ensure that we're actually getting a benefit out of this, before we commit to
    // storing a compressed blob.
    val totalCompressedSize = 4 + out.limit()
    if (totalCompressedSize < remaining) {
      stats.compressionBytesSavedStat.add(remaining - totalCompressedSize)
      stats.compressionRatioStat.add(remaining.toFloat / totalCompressedSize.toFloat)
      (Lz4.compressionFlags, Buf(Seq(Buf.ByteBuffer.Owned(lengthBuf), Buf.ByteBuffer.Owned(out))))
    } else {
      // It's not worth it to compress this block. Just return the original
      // uncompressed data.
      uncompressedBufferAndFlags(remaining, buf, stats)
    }
  }

  private def uncompressedBufferAndFlags(
    remaining: Int,
    buf: Buf,
    stats: CompressingMemcachedClientStats
  ): FlagsAndBuf = {
    stats.uncompressedBytesStat.add(remaining)
    stats.compressionSkippedCounter.incr()
    (Uncompressed.compressionFlags, buf)
  }

  override protected def decompress(flagsAndBuf: FlagsAndBuf): Buf = {
    val (flags, buf) = flagsAndBuf

    CompressionScheme.compressionType(flags) match {
      case Lz4 =>
        // Frame includes the length of the uncompressed contents
        val ba = Buf.ByteBuffer.Owned.extract(buf)
        val uncompressedLength = ba.getInt()
        val compressedData = ba.slice()
        val dest = ByteBuffer.allocate(uncompressedLength)
        Decompressor.decompress(compressedData, dest)

        dest.flip()

        Stats.decompressionAttemptedCounter.incr()
        Stats.decompressionBytesSavedStat.add(uncompressedLength - buf.length)
        Stats.decompressionRatioStat.add(uncompressedLength.toFloat / buf.length.toFloat)
        Buf.ByteBuffer.Owned(dest)
      case compressionScheme =>
        throw new IllegalStateException(
          s"Received $compressionScheme while Lz4 was expected for decompression")
    }
  }

}
