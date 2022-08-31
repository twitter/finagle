package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.tracing.Tracer
import com.twitter.util.Await
import com.twitter.util.Future
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

@State(Scope.Benchmark)
class PayloadSizeFilterBenchmark extends StdBenchAnnotations {

  private[this] val result = Future.value("yeah")
  private[this] val filter = new PayloadSizeFilter[String, String](
    NullStatsReceiver,
    "request_bytes",
    "response_bytes",
    _ => 5,
    _ => 5
  )
  private[this] val svc = filter.andThen(Service.constant(result))

  private[this] val noOpTracer = new Tracer {
    def record(record: Record): Unit = ()
    def sampleTrace(traceId: TraceId): Option[Boolean] = None
    override def isActivelyTracing(traceId: TraceId): Boolean = true
    def getSampleRate: Float = 0f
  }

  @Benchmark
  def applyWithTracing(): String = {
    Trace.letTracer(noOpTracer) {
      val res = svc("oh")
      Await.result(res)
    }
  }

  @Benchmark
  def applyWithoutTracing(): String = {
    Trace.letClear {
      val res = svc("oh")
      Await.result(res)
    }
  }

}
