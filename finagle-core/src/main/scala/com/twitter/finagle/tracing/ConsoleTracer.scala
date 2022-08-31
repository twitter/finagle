package com.twitter.finagle.tracing

import scala.Float.NaN

object ConsoleTracer extends Tracer {
  def record(record: Record): Unit = {
    println(record)
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None

  def getSampleRate: Float = NaN

  override def isActivelyTracing(traceId: TraceId): Boolean = true
}
