package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing.{Record, TraceId, Tracer}
import com.twitter.logging.Logger

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

  def sampleTrace(traceId: TraceId): Option[Boolean] = sampler.sampleTrace(traceId)

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
