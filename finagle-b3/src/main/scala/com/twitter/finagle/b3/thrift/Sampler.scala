package com.twitter.finagle.b3.thrift

import com.twitter.finagle.tracing.{Record, TraceId}

object Sampler {
  // Default is 0.001 = 0.1% (let one in a 1000nd pass)
  val DefaultSampleRate = 0.001f
}

/**
 * Decide if we should sample a particular trace or not.
 */
class Sampler {
  private[this] var sr = Sampler.DefaultSampleRate

  /**
   * Set the sample rate.
   *
   * How much to let through? For everything, use 1 = 100.00%
   */
  def setSampleRate(sampleRate: Float) = {
    if (sampleRate < 0 || sampleRate > 1) {
      throw new IllegalArgumentException("Sample rate not within the valid range of 0-1, was " + sr)
    }
    sr = sampleRate
  }

  /**
   * @return the current sample rate, 0.0-1.0
   */
  def sampleRate = sr

  /**
   * Should we drop this particular trace or send it on to Scribe?
   * True means keep.
   * False means drop.
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = {
    Some(math.abs(traceId.traceId.toLong) % 10000 < sr * 10000)
  }

  /**
   * Decides if we should record this record or not.
   * If this trace is marked as not sampled we just throw away all the records
   * If this trace is marked as None (no decision has been made), consult the sampleTrace impl
   * @return true if we should keep it, false for throw away
   */
  def sampleRecord(record: Record): Boolean = {
    record.traceId.sampled.getOrElse(sampleTrace(record.traceId).get)
  }
}
