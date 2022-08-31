package com.twitter.finagle.tracing

import scala.Float.NaN

/**
 * A no-op [[Tracer]].
 *
 * @note supplying this tracer to a finagle client or server will not prevent
 * trace information from being propagated to the next peer, but it will ensure
 * that the client or server does not log any trace information about this host.
 * If traces are being aggregated across your fleet, it will orphan subsequent
 * spans.
 */
class NullTracer extends Tracer {
  def record(record: Record): Unit = ()
  def sampleTrace(traceId: TraceId): Option[Boolean] = None
  def getSampleRate: Float = NaN
  override def isNull: Boolean = true
  override def isActivelyTracing(traceId: TraceId): Boolean = false
  override def toString: String = "NullTracer"
}

/**
 * A singleton instance of a no-op [[NullTracer]].
 */
object NullTracer extends NullTracer
