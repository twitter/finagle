package com.twitter.finagle.zipkin.core

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class SpanTest extends AnyFunSuite {
  test("Span should serialize properly") {
    val ann = ZipkinAnnotation(Time.now, "value", Endpoint(1, 2))
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
    val span = new Span(traceId, Some("service"), Some("name"), Seq(ann), Seq(), Endpoint(123, 123))

    val tspan = span.toThrift
    assert(tspan.isSetAnnotations)
    val host = tspan.getAnnotations.get(0).getHost
    assert(host.getService_name == "service")
    assert(tspan.isSetName)
    assert(tspan.getName == "name")
    !tspan.isSetBinary_annotations
    assert(tspan.isSetId)
    assert(tspan.getId == 123)
    assert(tspan.isSetParent_id)
    assert(tspan.getParent_id == 123)
    assert(tspan.isSetTrace_id)
    assert(tspan.getTrace_id == 123)
    assert(tspan.isDebug)
  }

  /**
   * The following tests check that the timestamp is set correctly under various conditions which
   * may occur depending on how the span is constructed and when it is evicted from the
   * DeadlineSpanMap.
   */
  test("The creation timestamp is older than the annotation timestamp") {
    Time.withCurrentTimeFrozen { tc =>
      val t1 = Time.now
      tc.advance(30.seconds)
      val t2 = Time.now

      val ann = ZipkinAnnotation(t2, "value", Endpoint(1, 2))
      val traceId =
        TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
      val span =
        Span(traceId, Some("service"), Some("name"), Seq(ann), Seq(), Endpoint(123, 123), t1)
      val tspan = span.toThrift
      assert(Time.fromMicroseconds(tspan.timestamp) == t1, "Incorrect span timestamp")
    }
  }

  test("The creation timestamp is newer than the annotation timestamp") {
    Time.withCurrentTimeFrozen { tc =>
      val t1 = Time.now
      tc.advance(30.seconds)
      val t2 = Time.now

      val ann = ZipkinAnnotation(t1, "value", Endpoint(1, 2))
      val traceId =
        TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
      val span =
        Span(traceId, Some("service"), Some("name"), Seq(ann), Seq(), Endpoint(123, 123), t2)
      val tspan = span.toThrift
      assert(Time.fromMicroseconds(tspan.timestamp) == t1, "Incorrect span timestamp")
    }
  }

  test("The creation timestamp is specified and there are no timestamped annotations") {
    Time.withCurrentTimeFrozen { tc =>
      val t1 = Time.now
      tc.advance(30.seconds)

      val traceId =
        TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
      val span =
        Span(traceId, Some("service"), Some("name"), Seq(), Seq(), Endpoint(123, 123), t1)
      val tspan = span.toThrift
      assert(Time.fromMicroseconds(tspan.timestamp) == t1, "Incorrect span timestamp")
    }
  }

  test("The creation timestamp is not specified and there are no timestamped annotations") {
    Time.withCurrentTimeFrozen { tc =>
      val t1 = Time.now

      val traceId =
        TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
      val span =
        Span(traceId, Some("service"), Some("name"), Seq(), Seq(), Endpoint(123, 123))

      tc.advance(30.seconds)
      val t2 = Time.now
      val tspan = span.toThrift
      assert(Time.fromMicroseconds(tspan.timestamp) == t1, "Incorrect span timestamp")
    }
  }
}
