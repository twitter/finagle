package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.stats.{DefaultStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{TraceId, Record, Tracer}
import com.twitter.finagle.zipkin.{host => Host, initialSampleRate => sampleRateFlag}
import com.twitter.util.events.{Event, Sink}

object ZipkinTracer {

  lazy val default: Tracer = mk()

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for trace events.
   */
  val Trace: Event.Type = new Event.Type { }

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
   * @param statsReceiver stats receiver to send successes/failures to
   */
  def mk(statsReceiver: StatsReceiver): Tracer =
    mk(Host().getHostName, Host().getPort, statsReceiver, Sampler.DefaultSampleRate)
}

/**
 * Tracer that supports sampling. Will pass through a subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
 *          to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 * @param sink where to send sampled trace events to.
 */
class SamplingTracer(
    underlyingTracer: Tracer,
    initialSampleRate: Float,
    sink: Sink)
  extends Tracer
{

  /**
   * Tracer that supports sampling. Will pass through a subset of the records.
   * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
   *          to the collector.
   * @param initialSampleRate Start off with this sample rate. Can be changed later.
   */
  def this(underlyingTracer: Tracer, initialSampleRate: Float) =
    this(underlyingTracer, initialSampleRate, Sink.default)

  /**
   * Tracer that supports sampling. Will pass through a subset of the records.
   */
  def this() = this(
    RawZipkinTracer(Host().getHostName, Host().getPort, DefaultStatsReceiver.scope("zipkin")),
    sampleRateFlag())

  private[this] val sampler = new Sampler
  setSampleRate(initialSampleRate)

  def sampleTrace(traceId: TraceId): Option[Boolean] = sampler.sampleTrace(traceId)

  def setSampleRate(sampleRate: Float): Unit = sampler.setSampleRate(sampleRate)
  def getSampleRate: Float = sampler.sampleRate

  def record(record: Record) {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
      sink.event(ZipkinTracer.Trace, objectVal = record.annotation)
    }
  }
}

class ZipkinTracer(tracer: RawZipkinTracer, initialRate: Float)
  extends SamplingTracer(tracer, initialRate)
