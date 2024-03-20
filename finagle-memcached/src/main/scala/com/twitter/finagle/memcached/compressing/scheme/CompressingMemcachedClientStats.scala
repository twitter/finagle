package com.twitter.finagle.memcached.compressing.scheme

import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.Stat
import com.twitter.finagle.stats.StatsReceiver

case class CompressingMemcachedClientStats private (
  compressionAttemptedCounter: Counter,
  compressionSkippedCounter: Counter,
  compressionBytesSavedStat: Stat,
  compressionRatioStat: Stat,
  uncompressedBytesStat: Stat,
  decompressionAttemptedCounter: Counter,
  decompressionBytesSavedStat: Stat,
  decompressionRatioStat: Stat)

object CompressingMemcachedClientStats {
  def apply(statsReceiver: StatsReceiver): CompressingMemcachedClientStats = {
    val compressionScope = statsReceiver.scope("compression")
    val decompressionScope = statsReceiver.scope("decompression")

    new CompressingMemcachedClientStats(
      compressionAttemptedCounter = compressionScope.counter("attempted"),
      compressionSkippedCounter = compressionScope.counter("skipped"),
      compressionBytesSavedStat = compressionScope.stat("bytesSaved"),
      compressionRatioStat = compressionScope.stat("ratio"),
      uncompressedBytesStat = compressionScope.stat("uncompressedBytes"),
      decompressionAttemptedCounter = decompressionScope.counter("attempted"),
      decompressionBytesSavedStat = decompressionScope.stat("bytesSaved"),
      decompressionRatioStat = decompressionScope.stat("ratio")
    )
  }
}
