package com.twitter.finagle.zipkin.thrift

import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.{SpanId, TraceId}
import com.twitter.util.{Future, MockTimer, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeadlineSpanMapTest extends FunSuite {

  test("DeadlineSpanMap should expire and log spans") {
    Time.withCurrentTimeFrozen { tc =>
      var spansLogged: Boolean = false
      val logger: Seq[Span] => Future[Unit] = { _ =>
        spansLogged = true
        Future.Done
      }

      val timer = new MockTimer
      val map = new DeadlineSpanMap(logger, 1.milliseconds, NullStatsReceiver, timer)
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

      val span = map.update(traceId)(_.setServiceName("service").setName("name"))
      tc.advance(10.seconds) // advance timer
      timer.tick() // execute scheduled event

      // span must have been logged
      assert(spansLogged)
    }
  }
}
