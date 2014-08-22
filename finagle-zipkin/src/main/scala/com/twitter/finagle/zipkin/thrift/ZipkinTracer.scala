package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.stats.{DefaultStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{TraceId, Record, Tracer}
import com.twitter.finagle.zipkin.{host => Host, initialSampleRate}

object ZipkinTracer {

  lazy val default = mk()

  /**
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   */
  @deprecated("Use mk() instead", "6.1.0")
  def apply(
    scribeHost: String = Host().getHostName,
    scribePort: Int = Host().getPort,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sampleRate: Float = Sampler.DefaultSampleRate
  ): Tracer.Factory = () => mk(scribeHost, scribePort, statsReceiver, sampleRate)

  /**
   * @param host Host to send trace data to
   * @param port Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   */
  def mk(
    host: String = Host().getHostName,
    port: Int = Host().getPort,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sampleRate: Float = Sampler.DefaultSampleRate
  ): Tracer =
    new ZipkinTracer(
      RawZipkinTracer(host, port, statsReceiver),
      sampleRate)

  /**
   * Util method since named parameters can't be called from Java
   * @param sr stats receiver to send successes/failures to
   */
  @deprecated("Use mk() instead", "6.1.0")
  def apply(sr: StatsReceiver): Tracer.Factory = () =>
    mk(Host().getHostName, Host().getPort, sr, Sampler.DefaultSampleRate)

  /**
   * Util method since named parameters can't be called from Java
   * @param sr stats receiver to send successes/failures to
   */
  def mk(statsReceiver: StatsReceiver): Tracer =
    mk(Host().getHostName, Host().getPort, statsReceiver, Sampler.DefaultSampleRate)
}

/**
 * Tracer that supports sampling. Will pass through a small subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 */
class SamplingTracer(underlyingTracer: Tracer, initialSampleRate: Float) extends Tracer {
  private[this] val sampler = new Sampler
  setSampleRate(initialSampleRate)

  def this() = this(
    RawZipkinTracer(Host().getHostName, Host().getPort, DefaultStatsReceiver.scope("zipkin")),
    initialSampleRate())

  def sampleTrace(traceId: TraceId) = sampler.sampleTrace(traceId)

  def setSampleRate(sampleRate: Float) = sampler.setSampleRate(sampleRate)
  def getSampleRate = sampler.sampleRate

  def record(record: Record) {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
    }
  }
}

class ZipkinTracer(tracer: RawZipkinTracer, initialRate: Float)
  extends SamplingTracer(tracer, initialRate)
