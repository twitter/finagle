package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.TraceId
import com.twitter.logging.Logger
import java.lang.{Long => JLong}
import scala.util.Random

object Sampler {
  // Default is 0.001 = 0.1% (let one in a 1000nd pass)
  val DefaultSampleRate = 0.001f

  // Maximum of the range of int values that are continuously
  // representable in a float without loss of precision.
  private val Multiplier = (1 << 24).toFloat
  private val BitMask = (Multiplier - 1).toInt

  /**
   * This salt is used to prevent traceId collisions between machines.
   * By giving each system a random salt, it is less likely that two
   * processes will sample the same subset of trace ids.
   */
  private val salt = new Random().nextInt()

  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}

/**
 * Decide if we should sample a particular trace or not.
 */
class Sampler(private var sr: Float) {
  private[this] val log = Logger(getClass.getName)

  def this() = this(Sampler.DefaultSampleRate)

  /**
   * Set the sample rate.
   *
   * How much to let through? For everything, use 1 = 100.00%
   */
  def setSampleRate(sampleRate: Float): Unit = {
    if (!validSampleRate(sampleRate)) {
      throw new IllegalArgumentException(
        "Sample rate not within the valid range of 0-1, was " + sampleRate
      )
    }
    sr = sampleRate
    log.info(s"Zipkin trace sample rate set to $sampleRate")
  }

  /**
   * @param sampleRate is this sample rate valid (0-1f range)?
   */
  def validSampleRate(sampleRate: Float): Boolean = sampleRate >= 0 && sampleRate <= 1

  /**
   * @return the current sample rate, 0.0-1.0
   */
  def sampleRate: Float = sr

  /**
   * Should we drop this particular trace or send it on to Scribe?
   * True means keep.
   * False means drop.
   * @param traceId check if this trace id passes the sampler
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = sampleTrace(traceId, sampleRate)

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
        // Notes:
        // - JLong.hashCode will fold the two 32-bit halves of the 64-bit trace ID, and we
        //   subsequently use low 24 bits of the result. This way, we actually use 48 bits of the
        //   ID for randomization. The theory is that trace IDs have a uniform distribution of bits,
        //   so this shouldn't matter, but it's a cheap way to ensure better randomization even in
        //   the case the uniformity does not hold.
        // - Since Multiplier is a power of 2, we can use `x & Sampler.BitMask` as a more efficient
        //   equivalent to `math.abs(x) % Sampler.Multiplier`.
        if (((JLong.hashCode(
            traceId.traceId.toLong) ^ Sampler.salt) & Sampler.BitMask) < sampleRate * Sampler.Multiplier)
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
      case None => false
    }
  }
}
