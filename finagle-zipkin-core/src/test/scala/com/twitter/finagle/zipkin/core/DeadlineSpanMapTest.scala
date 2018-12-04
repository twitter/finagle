package com.twitter.finagle.zipkin.core

import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.thrift.thrift.Constants
import com.twitter.finagle.tracing.{SpanId, TraceId}
import com.twitter.util.{Duration, Future, MockTimer, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeadlineSpanMapTest extends FunSuite {

  /**
   * Tests state transition sequence (iv): live -> flushed -> logged.
   * See the comment in DeadlineSpanMap.scala for more details.
   */
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

  /**
   * Tests state transition sequence (i): live -> on hold -> complete -> logged.
   * See the comment in DeadlineSpanMap.scala for more details.
   */
  test("The hold timer catches late spans and on expiry sets the span to completed") {
    Time.withCurrentTimeFrozen { tc =>
      var spansLoggedCount = 0
      var annotationCount = 0
      val logger: Seq[Span] => Future[Unit] = { spans =>
        spans.foreach { span =>
          annotationCount += span.annotations.length
        }
        spansLoggedCount += 1
        Future.Done
      }

      val timer = new MockTimer
      val ttl: Duration = 10.milliseconds
      val hold: Duration = 1.milliseconds
      val map = new DeadlineSpanMap(logger, ttl, NullStatsReceiver, timer, hold)
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

      // Add an annotation to transition the span to hold state.
      map.update(traceId)(
        _.addAnnotation(
          ZipkinAnnotation(Time.now, Constants.CLIENT_RECV, Endpoint.Unknown)
        )
      )

      tc.advance(1.milliseconds) // advance timer but not beyond the hold deadline
      timer.tick()

      // Add another annotation.
      map.update(traceId)(
        _.addAnnotation(
          ZipkinAnnotation(Time.now, "Extra annotation", Endpoint.Unknown)
        )
      )

      tc.advance(1.milliseconds) // advance timer beyond the hold expiry deadline, but not ttl
      timer.tick()

      // Span must have been logged exactly once.
      assert(spansLoggedCount == 1, "Wrong number of calls to log spans")
      assert(annotationCount == 2, "Wrong number of annotations")
    }
  }

  /**
   * Tests state transition sequence (ii): live -> on hold -> complete -> flushed -> logged.
   * See the comment in DeadlineSpanMap.scala for more details.
   */
  test("The hold timer transitions span to completed state, flushable on ttl expiry") {
    Time.withCurrentTimeFrozen { tc =>
      var spansLoggedCount = 0
      var annotationCount = 0
      val logger: Seq[Span] => Future[Unit] = { spans =>
        spans.foreach { span =>
          annotationCount += span.annotations.length
        }
        spansLoggedCount += 1
        Future.Done
      }

      val timer = new MockTimer
      val ttl: Duration = 2.milliseconds
      val hold: Duration = 1.milliseconds
      val map = new DeadlineSpanMap(logger, ttl, NullStatsReceiver, timer, hold)
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

      // Add an annotation to transition the span to hold state.
      map.update(traceId)(
        _.addAnnotation(
          ZipkinAnnotation(Time.now, Constants.CLIENT_RECV, Endpoint.Unknown)
        )
      )

      tc.advance(3.milliseconds) // advance timer beyond both hold and ttl deadlines
      timer.tick() // execute scheduled event

      // Span must have been logged exactly once.
      assert(spansLoggedCount == 1, "Wrong number of calls to log spans")

      // Flushing adds a "finagle.flush" annotation.
      assert(annotationCount == 2, "Wrong number of annotations")
    }
  }

  /**
   * Tests state transition sequence (iii): live -> on hold -> flushed -> logged.
   * See the comment in DeadlineSpanMap.scala for more details.
   */
  test("Even if on hold, the span is flushed if ttl expires first") {
    Time.withCurrentTimeFrozen { tc =>
      var spansLoggedCount = 0
      var annotationCount = 0
      val logger: Seq[Span] => Future[Unit] = { spans =>
        spans.foreach { span =>
          annotationCount += span.annotations.length
        }
        spansLoggedCount += 1
        Future.Done
      }

      val timer = new MockTimer
      val ttl: Duration = 1.milliseconds
      val hold: Duration = 2.milliseconds
      val map = new DeadlineSpanMap(logger, ttl, NullStatsReceiver, timer, hold)
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

      // Add an annotation to transition the span to hold state.
      map.update(traceId)(
        _.addAnnotation(
          ZipkinAnnotation(Time.now, Constants.CLIENT_RECV, Endpoint.Unknown)
        )
      )

      tc.advance(1.milliseconds) // advance timer beyond the ttl
      timer.tick() // execute scheduled event

      // Add another annotation, which will be logged separately.
      map.update(traceId)(
        _.addAnnotation(
          ZipkinAnnotation(Time.now, "Extra annotation", Endpoint.Unknown)
        )
      )

      tc.advance(2.milliseconds) // advance timer beyond the ttl
      timer.tick() // execute scheduled event

      // Span must have been logged twice.
      assert(spansLoggedCount == 2, "Wrong number of calls to log spans")

      // Flushing adds a "finagle.flush" annotation and we have flushed twice.
      assert(annotationCount == 4, "Wrong number of annotations")
    }
  }
}
