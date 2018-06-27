package com.twitter.finagle.tracing

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class TraceIdBenchmark extends StdBenchAnnotations {

  private[this] val traceId = TraceId(
    _traceId = None,
    _parentId = None,
    spanId = SpanId(555L),
    _sampled = None,
    flags = Flags(), // debug is not set
    traceIdHigh = None
  )

  @Benchmark
  def sampled(): Option[Boolean] =
    traceId.sampled

}
