package com.twitter.finagle.tracing

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.thrift.ClientId
import com.twitter.util.Time
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

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

  @Param(Array("3"))
  var num = 3

  @OperationsPerInvocation(50)
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

  @OperationsPerInvocation(50)
  @Benchmark
  def contexts1(hole: Blackhole): Unit =
    Trace.letId(traceId, terminal = false) { contexts0(hole) }

  @OperationsPerInvocation(50)
  @Benchmark
  def contexts2(hole: Blackhole): Unit =
    clientId.asCurrent { contexts1(hole) }

  @OperationsPerInvocation(50)
  @Benchmark
  def contexts3(hole: Blackhole): Unit =
    Contexts.broadcast.let(Deadline, deadline) { contexts2(hole) }

  @OperationsPerInvocation(50)
  @Benchmark
  def contexts4(hole: Blackhole): Unit =
    Contexts.broadcast.letClear(Deadline) { contexts3(hole) }

  @OperationsPerInvocation(50)
  @Benchmark
  def contexts5(hole: Blackhole): Unit =
    Trace.letTracer(NullTracer) { contexts4(hole) }

  private[this] def traced(n: Int): Boolean =
    if (n == 0) {
      Trace.letId(traceId) {
        Trace.isActivelyTracing
      }
    } else {
      Trace.letTracer(ConsoleTracer) {
        traced(n - 1)
      }
    }

  @Benchmark
  def isActivelyTracing: Boolean =
    traced(num)

  @Benchmark
  @Warmup(iterations = 3)
  @Measurement(iterations = 5)
  @Threads(4)
  def recordAnnotationSlow(): Unit =
    Trace.letTracer(TraceBenchmark.NoopAlwaysSamplingTracer) {
      if (Trace.isActivelyTracing) {
        Trace.record(Annotation.WireSend)
        Trace.record(Annotation.WireSend)
        Trace.record(Annotation.WireSend)
        Trace.record(Annotation.WireSend)
      }
    }

  @Benchmark
  @Warmup(iterations = 3)
  @Measurement(iterations = 5)
  @Threads(4)
  def recordAnnotationFast(): Unit =
    Trace.letTracer(TraceBenchmark.NoopAlwaysSamplingTracer) {
      val trace = Trace()
      if (trace.isActivelyTracing) {
        trace.record(Annotation.WireSend)
        trace.record(Annotation.WireSend)
        trace.record(Annotation.WireSend)
        trace.record(Annotation.WireSend)
      }
    }
}

private object TraceBenchmark {
  private val NoopAlwaysSamplingTracer: Tracer = new Tracer {
    def record(record: Record): Unit = ()
    def sampleTrace(traceId: TraceId): Option[Boolean] = Tracer.SomeTrue
    def getSampleRate: Float = 1f
    override def isActivelyTracing(traceId: TraceId): Boolean = true
  }
}
