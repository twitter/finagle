package com.twitter.finagle.zipkin

import com.twitter.app.Flaggable
import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.core.Sampler

/**
 * An implementation of a [[Flaggable]][Float] that is used with [[initialSampleRate]] to verify
 * that the sample rate is in the range [0.0, 1.0]. If it's not in that range this will throw an
 * [[IllegalArgumentException]].
 */
private object InitialSampleRateFlaggable extends Flaggable[Float] {

  override def parse(s: String): Float = {
    val sampleRate = s.toFloat
    if (sampleRate < 0.0f || sampleRate > 1.0f) {
      throw new IllegalArgumentException(
        s"${initialSampleRate.name} flag's value should be between 0.0 and 1.0 found $sampleRate")
    }
    sampleRate
  }
}

object initialSampleRate
    extends GlobalFlag[Float](Sampler.DefaultSampleRateInternal, "Initial sample rate")(
      InitialSampleRateFlaggable)
