package com.twitter.finagle.zipkin.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.finagle.tracing.{TraceId, SpanId}
import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.MockTimer
import com.twitter.util.Time

class DeadlineSpanMapSpec extends SpecificationWithJUnit with Mockito {

  "DeadlineSpanMap" should {
    "expire and log spans" in Time.withCurrentTimeFrozen { tc =>
      val tracer = mock[RawZipkinTracer]
      val timer = new MockTimer

      val map = new DeadlineSpanMap(tracer, 1.milliseconds, NullStatsReceiver, timer)
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
      val f = { span: Span =>
        span.copy(_name = Some("name"), _serviceName = Some("service"))
      }

      val span = map.update(traceId)(f)
      tc.advance(10.seconds) // advance timer
      timer.tick() // execute scheduled event

      // span must have been removed and logged
      map.remove(traceId) mustEqual None
      there was one(tracer).logSpan(span)

      timer.stop()
    }
  }
}
