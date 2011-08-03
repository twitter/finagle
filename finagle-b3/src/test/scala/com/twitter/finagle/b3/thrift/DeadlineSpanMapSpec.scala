package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.tracing.{TraceId, SpanId}
import com.twitter.conversions.time._
import com.twitter.finagle.util.Timer

class DeadlineSpanMapSpec extends Specification with Mockito {

  "DeadlineSpanMap" should {
    "expire and log spans" in {
      val tracer = mock[BigBrotherBirdTracer]
      Timer.default.acquire()
      val map = new DeadlineSpanMap(tracer, 1.milliseconds)

      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
      val f = { span: Span =>
        span.copy(_name = Some("name"), _serviceName = Some("service"))
      }
      val span = map.update(traceId)(f)

      Thread.sleep(30) // just in case...
      Timer.default.stop()
      map.remove(traceId).isDefined mustEqual false
      there was one(tracer).logSpan(span)
    }
  }
}