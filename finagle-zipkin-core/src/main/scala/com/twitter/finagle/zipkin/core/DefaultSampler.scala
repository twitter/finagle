package com.twitter.finagle.zipkin.core

import com.twitter.finagle.zipkin.initialSampleRate
import com.twitter.logging.Logger

/**
 * The DefaultSampler uses the [[initialSampleRate]] flag's value as the sample rate. If the flag is
 * not set by the user it defaults to the sample rate provided by [[Sampler.sampleRate]].
 * Pass this singleton as the [[Sampler]] when constructing the
 * [[com.twitter.finagle.tracing.Tracer Tracer]].
 */
object DefaultSampler extends Sampler {

  private[this] val log = Logger(getClass.getName)

  /**
   * Allows the users to explicitly override the sample rate. This will not work if the
   * [[initialSampleRate]] flag is manually set by the user.
   *
   * @param sampleRate the new sample rate. Value should be in the range [0.0, 1.0].
   */
  override def setSampleRate(sampleRate: Float): Unit = {
    if (!initialSampleRate.isDefined) {
      log.info(s"Set default Zipkin trace sample rate to $sampleRate")
      super.setSampleRate(sampleRate)
    } else {
      log.warning(
        s"Not setting the Zipkin trace sample rate to $sampleRate as the flag ${initialSampleRate.name} is set to ${initialSampleRate()}")
    }
  }

  /**
   * Current sample rate.
   *
   * @return the [[initialSampleRate]] if set by the user else return [[Sampler.sampleRate]].
   */
  override def sampleRate: Float = initialSampleRate.get match {
    case Some(sr) => sr
    case None => super.sampleRate
  }
}
