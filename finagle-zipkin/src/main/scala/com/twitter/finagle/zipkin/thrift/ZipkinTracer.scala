package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{TraceId, Record, Tracer}
import com.twitter.finagle.util.ManagedTimer
import collection.mutable.{SynchronizedMap, HashMap}

object ZipkinTracer {

  // to make sure we only create one instance of the tracer and sampler per host and port
  private[this] val map =
    new HashMap[String, ZipkinTracer] with SynchronizedMap[String, ZipkinTracer]

  /**
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   */
  def apply(scribeHost: String = "localhost",
            scribePort: Int = 1463,
            statsReceiver: StatsReceiver = NullStatsReceiver,
            sampleRate: Float = Sampler.DefaultSampleRate): Tracer.Factory = {

    val mTimer = ManagedTimer.toTwitterTimer
    val tracer = map.getOrElseUpdate(scribeHost + ":" + scribePort, {
      val raw = new RawZipkinTracer(
        scribeHost, scribePort, statsReceiver.scope("zipkin"), mTimer.make())
      new ZipkinTracer(raw, sampleRate)
    })

    h => {
      tracer.acquire()
      h.onClose {
        tracer.release()
      }
      tracer
    }
  }

  /**
   * Util method since named parameters can't be called from Java
   * @param sr stats receiver to send successes/failures to
   */
  def apply(sr: StatsReceiver): Tracer.Factory = apply(statsReceiver = sr)
}

/**
 * Zipkin tracer that supports sampling. Will pass through a small subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 */
class ZipkinTracer(underlyingTracer: RawZipkinTracer, initialSampleRate: Float) extends Tracer {
  private[this] val sampler = new Sampler
  setSampleRate(initialSampleRate)

  def acquire() = underlyingTracer.acquire()

  def sampleTrace(traceId: TraceId) = sampler.sampleTrace(traceId)

  def setSampleRate(sampleRate: Float) = sampler.setSampleRate(sampleRate)

  def record(record: Record) {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
    }
  }

  override def release() = underlyingTracer.release()
}
