package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.zipkin.core.Sampler
import com.twitter.finagle.zipkin.initialSampleRate
import com.twitter.logging.Logger

/**
 * The DefaultSampler provides a default sample rate that can be overridden
 * To override the default, pass this singleton as the Sampler when constructing
 * the Tracer.
 */
object DefaultSampler extends Sampler {

  private[this] val log = Logger(getClass.getName)

  /**
   * Default sample rate is the initialSampleRate flag value if set.
   */
  @volatile var defaultSampleRate: Option[Float] = initialSampleRate.get

  /**
   * Default sample rate may be explicitly overridden unless the initialSampleRate flag is set.
   * @param sampleRate
   */
  override def setSampleRate(sampleRate: Float): Unit = {
    if (!initialSampleRate.isDefined) {
      defaultSampleRate = Some(sampleRate)
      log.info(s"Set default Zipkin trace sample rate to $sampleRate")
    }
  }

  override def sampleRate: Float = defaultSampleRate match {
    case Some(sr) => sr
    case None => Sampler.DefaultSampleRate
  }

  private[thrift] def reset(): Unit = {
    defaultSampleRate = initialSampleRate.get
  }
}
