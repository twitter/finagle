package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.tracing.{Record, TraceId}
import scala.util.Random

object Sampler {
  // Default is 0.001 = 0.1% (let one in a 1000nd pass)
  val DefaultSampleRate = 0.001f

  /**
   * This salt is used to prevent traceId collisions between machines.
   * By giving each system a random salt, it is less likely that two
   * processes will sample the same subset of trace ids.
   */
  private val salt = new Random().nextLong()

  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}

/**
 * Decide if we should sample a particular trace or not.
 */
class Sampler {
  @volatile
  private[this] var sr = Sampler.DefaultSampleRate

  /**
   * Set the sample rate.
   *
   * How much to let through? For everything, use 1 = 100.00%
   */
  def setSampleRate(sampleRate: Float) = {
    if (!validSampleRate(sampleRate)) {
      throw new IllegalArgumentException("Sample rate not within the valid range of 0-1, was " + sampleRate)
    }
    sr = sampleRate
  }

  /**
   * @param sampleRate is this sample rate valid (0-1f range)?
   */
  def validSampleRate(sampleRate: Float): Boolean = sampleRate >= 0 && sampleRate <= 1

  /**
   * @return the current sample rate, 0.0-1.0
   */
  def sampleRate = sr

  /**
   * Should we drop this particular trace or send it on to Scribe?
   * True means keep.
   * False means drop.
   * @param traceId check if this trace id passes the sampler
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = sampleTrace(traceId, sr)

  /**
   * Should we drop this particular trace or send it on to Scribe?
   * True means keep.
   * False means drop.
   * @param traceId check if this trace id passes the sampler
   * @param sampleRate don't use the sampler's sample rate, instead use this one directly
   */
  def sampleTrace(traceId: TraceId, sampleRate: Float): Option[Boolean] = {
    traceId.sampled match {
      case None =>
        if (math.abs(traceId.traceId.toLong^Sampler.salt)%10000 < sampleRate*10000)
          Sampler.SomeTrue
        else
          Sampler.SomeFalse
      case sample @ Some(_) =>
        sample
    }
  }

  /**
   * Decides if we should record this record or not.
   * If this trace is marked as not sampled we just throw away all the records
   * If this trace is marked as None (no decision has been made), consult the sampleTrace impl
   * @return true if we should keep it, false for throw away
   */
  def sampleRecord(record: Record): Boolean = {
    sampleTrace(record.traceId) match {
      case Some(sampled) => sampled
      case None          => false
    }
  }
}
