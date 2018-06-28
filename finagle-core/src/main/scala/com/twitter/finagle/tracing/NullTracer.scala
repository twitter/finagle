package com.twitter.finagle.tracing

/**
 * A no-op [[Tracer]].
 */
class NullTracer extends Tracer {
  def record(record: Record): Unit = ()
  def sampleTrace(traceId: TraceId): Option[Boolean] = None
  override def isNull: Boolean = true
  override def isActivelyTracing(traceId: TraceId): Boolean = false
  override def toString: String = "NullTracer"
}

/**
 * A singleton instance of a no-op [[NullTracer]].
 */
object NullTracer extends NullTracer
