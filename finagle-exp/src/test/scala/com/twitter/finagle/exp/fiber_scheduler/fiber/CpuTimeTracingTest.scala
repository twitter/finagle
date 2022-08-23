package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.tracing.SpanId
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class CpuTimeTracingTest extends FiberSchedulerSpec {

  import CpuTimeTracing._

  "returns a null start" - {
    "when Trace.enabled is false" in {
      val c = new CpuTimeTracing("test", 100)
      assert(c() == nullStart)
    }
    "when tracing percentage is zero" in
      withTraceEnabled {
        val c = new CpuTimeTracing("test", 0)
        assert(c() == nullStart)
      }
    "tracer is not actively tracking" in
      withTraceEnabled {
        Trace.letTracer(NullTracer) {
          val c = new CpuTimeTracing("test", 100)
          assert(c() == nullStart)
        }
      }
  }

  "doesn't return a null start if all conditions are met, regardless of the id" in
    withTraceEnabled {
      for (i <- 0 until 10) {
        Trace.letTracerAndId(new BufferingTracer, traceId(i)) {
          val c = new CpuTimeTracing("test", 100)
          assert(c() != nullStart)
        }
      }
    }

  "selects a percentage of the traces" in
    withTraceEnabled {
      Trace.letTracer(new BufferingTracer) {
        val c = new CpuTimeTracing("test", 50)
        var selected = 0
        var notSelected = 0
        for (i <- 0 until 100) {
          Trace.letId(traceId(i)) {
            if (c() != nullStart) {
              selected += 1
            } else {
              notSelected += 1
            }
          }
        }
        // selection isn't precise, use 15% error margin
        assert(Math.abs(selected - notSelected) < 15)
      }
    }

  "records the cpu time annotation" in
    withTraceEnabled {
      val tracer = new BufferingTracer
      Trace.letTracerAndId(tracer, traceId()) {
        val c = new CpuTimeTracing("test", 100)
        val start = c()
        val stop = start()
        Math.log(3289)
        stop()
        val records = tracer.toList
        assert(records.size == 1)
        records.head.annotation match {
          case BinaryAnnotation(key, value: Long) =>
            assert(key.endsWith("test"))
            assert(value > 0)
          case _ =>
            fail()
        }
      }
    }

  def withTraceEnabled(f: => Unit) = {
    Trace.enable()
    try f
    finally Trace.disable()
  }

  def traceId(id: Long = 0L, sampled: Boolean = true) =
    TraceId(
      traceId = Some(SpanId(id)),
      parentId = None,
      spanId = SpanId(0),
      sampled = Some(sampled)
    )
}
