package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing.{Annotation => FinagleAnnotation, _}
import com.twitter.logging.Logger
import com.twitter.util.Time

/**
 * Tracer that supports sampling. Will pass through a subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
 *          to the collector.
 * @param sampler Sampler that decides whether to sample a trace
 */
class SamplingTracer(underlyingTracer: Tracer, sampler: Sampler) extends Tracer {

  def this(underlyingTracer: Tracer, initialSampleRate: Float) = {
    this(underlyingTracer, new Sampler(initialSampleRate))
  }

  private[this] val log = Logger(getClass.getName)

  private[this] def samplingRecord(traceId: TraceId): Record = {
    new Record(
      traceId,
      Time.now,
      FinagleAnnotation.BinaryAnnotation("zipkin.sampling_rate", sampler.sampleRate.toDouble),
      None)
  }

  /**
   * Makes the sampling decision and records the sampling rate if we're tracing
   * @note sampleTrace should only be called when the tracing decision has not already been made
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = {
    val st = sampler.sampleTrace(traceId)
    if (!traceId.sampled.contains(true) && st.contains(true)) {
      record(samplingRecord(traceId))
    }
    st
  }

  def setSampleRate(sampleRate: Float): Unit = {
    sampler.setSampleRate(sampleRate)
    log.info(s"Zipkin trace sample rate set to $sampleRate")
  }

  def getSampleRate: Float = sampler.sampleRate

  def record(record: Record): Unit = {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
    }
  }
}
