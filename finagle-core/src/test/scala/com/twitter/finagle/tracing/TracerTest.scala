package com.twitter.finagle.tracing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TracerTest extends FunSuite {
  test("Combined tracers sample correctly") {
    case class TestTracer(res: Option[Boolean]) extends Tracer {
      def record(record: Record) {}
      def sampleTrace(traceId: TraceId): Option[Boolean] = res
    }
    val id = TraceId(None, None, SpanId(0L), None)
    assert(BroadcastTracer(
      Seq(TestTracer(None), TestTracer(None), TestTracer(None))
    ).sampleTrace(id) == None, "If all None returns None")

    assert(BroadcastTracer(
      Seq(TestTracer(Some(true)), TestTracer(None), TestTracer(None))
    ).sampleTrace(id) == Some(true), "If one Some(true) returns Some(true)")

    assert(BroadcastTracer(
      Seq(TestTracer(Some(true)), TestTracer(Some(false)), TestTracer(None))
    ).sampleTrace(id) == Some(true), "If one Some(true) returns Some(true)")

    assert(BroadcastTracer(
      Seq(TestTracer(None), TestTracer(Some(false)), TestTracer(None))
    ).sampleTrace(id) == None, "If one Some(false) returns None")

    assert(BroadcastTracer(
      Seq(TestTracer(Some(false)), TestTracer(Some(false)), TestTracer(Some(false)))
    ).sampleTrace(id) == Some(false), "If all Some(false) returns Some(false)")
  }

  test("check equality of tracers") {
    val previous = DefaultTracer.self
    DefaultTracer.self = NullTracer

    val tracer = DefaultTracer
    assert(tracer == NullTracer, "Can't detect that tracer is NullTracer")

    DefaultTracer.self = ConsoleTracer
    assert(tracer != NullTracer, "Can't detect that tracer isn't NullTracer anymore")
    assert(tracer == ConsoleTracer, "Can't detect that tracer is ConsoleTracer")

    // Restore initial state
    DefaultTracer.self = previous
  }
}
