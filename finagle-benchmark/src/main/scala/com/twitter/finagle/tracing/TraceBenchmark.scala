package com.twitter.finagle.tracing

import com.twitter.finagle.Deadline
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.thrift.ClientId
import com.twitter.util.Time
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@OperationsPerInvocation(50)
@State(Scope.Benchmark)
class TraceBenchmark extends StdBenchAnnotations {

  private[this] val traceId = TraceId(
    Some(SpanId(5L)),
    Some(SpanId(6L)),
    SpanId(7L),
    None,
    Flags.apply()
  )

  // Note that this needs to == to @OperationsPerInvocation above
  private[this] val Iterations = 50

  private[this] val clientId = ClientId("bench")

  private[this] val deadline = Deadline(Time.Top, Time.Top)

  @Benchmark
  def contexts0(hole: Blackhole): Unit = {
    // Because Contexts are only scoped within a `let`, we want
    // to amortize the cost of setting up the context and try
    // to focus this measurement on the call to `Trace.id`.
    var i = 0
    while (i < Iterations) {
      hole.consume(Trace.id)
      i += 1
    }
  }

  @Benchmark
  def contexts1(hole: Blackhole): Unit =
    Trace.letId(traceId, terminal = false) { contexts0(hole) }

  @Benchmark
  def contexts2(hole: Blackhole): Unit =
    clientId.asCurrent { contexts1(hole) }

  @Benchmark
  def contexts3(hole: Blackhole): Unit =
    Contexts.broadcast.let(Deadline, deadline) { contexts2(hole) }

  @Benchmark
  def contexts4(hole: Blackhole): Unit =
    Contexts.broadcast.letClear(Deadline) { contexts3(hole) }

  @Benchmark
  def contexts5(hole: Blackhole): Unit =
    Trace.letTracer(NullTracer) { contexts4(hole) }

}
