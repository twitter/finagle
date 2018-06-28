package com.twitter.finagle.tracing

object ConsoleTracer extends Tracer {
  def record(record: Record): Unit = {
    println(record)
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None

  override def isActivelyTracing(traceId: TraceId): Boolean = true
}
