package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing.{TraceId, Record, Tracer}

/**
 * Tracer that supports sampling. Will pass through a subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
 *          to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 */
class SamplingTracer(underlyingTracer: Tracer, initialSampleRate: Float) extends Tracer {

  private[this] val sampler = new Sampler
  setSampleRate(initialSampleRate)

  def sampleTrace(traceId: TraceId): Option[Boolean] = sampler.sampleTrace(traceId)

  def setSampleRate(sampleRate: Float): Unit = sampler.setSampleRate(sampleRate)
  def getSampleRate: Float = sampler.sampleRate

  def record(record: Record): Unit = {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
    }
  }
}
