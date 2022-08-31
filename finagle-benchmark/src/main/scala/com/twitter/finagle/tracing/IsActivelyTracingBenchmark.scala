package com.twitter.finagle.tracing

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.OperationsPerInvocation
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

@OperationsPerInvocation(50)
@State(Scope.Benchmark)
class IsActivelyTracingBenchmark extends StdBenchAnnotations {

  // We don't care about the ??? because it will not be executed
  private[this] val tracer = new Tracer {

    def record(record: Record): Unit = ???

    def sampleTrace(traceId: TraceId): Option[Boolean] = ???

    def getSampleRate: Float = ???
  }

  private[this] val traceIdWithSampled = TraceId(
    Some(SpanId(5L)),
    Some(SpanId(6L)),
    SpanId(7L),
    Some(true),
    Flags.apply()
  )

  private[this] val traceIdWithDebug = traceIdWithSampled.copy(flags = Flags.apply(Flags.Debug))
  private[this] val traceIdWithoutSampled = traceIdWithSampled.copy(_sampled = None)

  // Note that this needs to == to @OperationsPerInvocation above
  private[this] val Iterations = 50

  private[this] def timeIsActivelyTracing(hole: Blackhole) = {
    // Because Contexts are only scoped within a `let`, we want
    // to amortize the cost of setting up the context and try
    // to focus this measurement on the call to `Trace.isActivelyTracing`.
    var i = 0
    while (i < Iterations) {
      hole.consume(Trace.isActivelyTracing)
      i += 1
    }
  }

  @Benchmark
  def oneTracerWithSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) {
      Trace.letId(traceIdWithSampled) { timeIsActivelyTracing(hole) }
    }
  }

  @Benchmark
  def oneTracerWithDebug(hole: Blackhole) = {
    Trace.letTracer(tracer) {
      Trace.letId(traceIdWithDebug) { timeIsActivelyTracing(hole) }
    }
  }

  @Benchmark
  def oneTracerWithoutSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) {
      Trace.letId(traceIdWithoutSampled) { timeIsActivelyTracing(hole) }
    }
  }

  @Benchmark
  def twoTracersWithSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) { oneTracerWithSampled(hole) }
  }

  @Benchmark
  def twoTracersWithDebug(hole: Blackhole) = {
    Trace.letTracer(tracer) { oneTracerWithDebug(hole) }
  }

  @Benchmark
  def twoTracersWithoutSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) { oneTracerWithoutSampled(hole) }
  }

  @Benchmark
  def threeTracersWithSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) { twoTracersWithSampled(hole) }
  }

  @Benchmark
  def threeTracersWithDebug(hole: Blackhole) = {
    Trace.letTracer(tracer) { twoTracersWithDebug(hole) }
  }

  @Benchmark
  def threeTracersWithoutSampled(hole: Blackhole) = {
    Trace.letTracer(tracer) { twoTracersWithoutSampled(hole) }
  }
}
