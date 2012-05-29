package com.twitter.finagle.b3.thrift

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{TraceId, Record, Tracer}
import com.twitter.finagle.util.FinagleTimer
import collection.mutable.{SynchronizedMap, HashMap}

object BigBrotherBirdTracer {

  // to make sure we only create one instance of the tracer and sampler per host and port
  private[this] val map =
    new HashMap[String, BigBrotherBirdTracer] with SynchronizedMap[String, BigBrotherBirdTracer]

  /**
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   * @deprecated Please use ZipkinTracer in the finagle-zipkin module instead.
   */
  def apply(scribeHost: String = "localhost",
            scribePort: Int = 1463,
            statsReceiver: StatsReceiver = NullStatsReceiver,
            sampleRate: Float = Sampler.DefaultSampleRate): Tracer.Factory = {

    val mTimer = FinagleTimer.getManaged
    val tracer = map.getOrElseUpdate(scribeHost + ":" + scribePort, {
      val raw = new RawBigBrotherBirdTracer(
        scribeHost, scribePort, statsReceiver.scope("b3"), mTimer.make())
      new BigBrotherBirdTracer(raw, sampleRate)
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
 * BigBrotherBird tracer that supports sampling. Will pass through a small subset of the records.
 * @param underlyingTracer Underlying B3 tracer that accumulates the traces and sends off to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 */
class BigBrotherBirdTracer(underlyingTracer: RawBigBrotherBirdTracer, initialSampleRate: Float) extends Tracer {
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
