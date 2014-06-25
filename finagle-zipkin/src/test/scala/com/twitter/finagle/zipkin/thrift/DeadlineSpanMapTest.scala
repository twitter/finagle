package com.twitter.finagle.zipkin.thrift

import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSuite
import com.twitter.finagle.MockTimer
import com.twitter.util.TimeConversions._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.{SpanId, TraceId}
import org.mockito.Mockito.verify
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.twitter.util.TimeControl

@RunWith(classOf[JUnitRunner])
class DeadlineSpanMapTest extends FunSuite with MockitoSugar {

  test("DeadlineSpanMap should expire and log spans") { tc: TimeControl =>
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
    assert(map.remove(traceId) === None)
    verify(tracer).logSpan(span)

    timer.stop()
  }
}
