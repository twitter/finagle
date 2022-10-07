package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Annotation
import com.twitter.jvm.Jvm
import com.twitter.jvm.Gc
import com.twitter.util.TimeControl
import com.twitter.util.Promise
import com.twitter.util.Time
import com.twitter.util.Duration
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class MkJvmFilterTest extends AnyFunSuite with MockitoSugar {
  private class JvmHelper {
    var gcEvents: List[Gc] = Nil

    def mockGc(count: Int, duration: Duration = 1.second): Unit = {
      gcEvents ::= Gc(count, "pcopy", Time.now, duration)
    }

    private val jvm = mock[Jvm]
    when(jvm.monitorGcs(any[Duration])).thenReturn { since: Time =>
      gcEvents.takeWhile(_.timestamp > since)
    }

    val mkFilter = new MkJvmFilter(jvm)
    private val filter = mkFilter[String, String]()

    val service: Service[String, String] = mock[Service[String, String]]
    val p = new Promise[String]
    when(service(any[String])).thenReturn(p)
    val filtered: Service[String, String] = filter.andThen(service)

    def traced(f: => Unit): Seq[Record] = {
      val b = new BufferingTracer
      Trace.letTracer(b)(f)
      b.toSeq
    }
  }

  test("JvmFilter should Record overlapping Gcs") {
    Time.withCurrentTimeFrozen { tc: TimeControl =>
      val h = new JvmHelper
      import h._

      val trace = traced {
        assert(!filtered("ok").isDefined)
        verify(service).apply("ok")
        tc.advance(1.second)
        mockGc(1)
        tc.advance(1.second)
        p.setValue("ko")
      }

      assert(
        trace == Seq(
          Record(
            Trace.id,
            1.second.ago,
            Annotation.Message("GC Start"),
            None
          ),
          Record(
            Trace.id,
            Time.now,
            Annotation.Message("GC End"),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_count", 1),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_ms", 1.second.inMilliseconds),
            None
          )
        )
      )
    }
  }

  test("JvmFilter should not record nonoverlapping Gcs") {
    Time.withCurrentTimeFrozen { tc: TimeControl =>
      val h = new JvmHelper
      import h._

      val trace = traced {
        // this gc event happens before the request occurs,
        // and as such should not be included in the trace
        mockGc(1, 1.second)
        tc.advance(10.seconds)
        assert(!filtered("ok").isDefined)
        tc.advance(1.second)
        mockGc(2, 2.seconds)
        tc.advance(2.seconds)
        p.setValue("ko")
      }

      assert(
        trace == Seq(
          Record(
            Trace.id,
            2.seconds.ago,
            Annotation.Message("GC Start"),
            None
          ),
          Record(
            Trace.id,
            Time.now,
            Annotation.Message("GC End"),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_count", 1),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_ms", 2.seconds.inMilliseconds),
            None
          )
        )
      )
    }
  }

  test("JvmFilter with no overlapping gcs") {
    Time.withCurrentTimeFrozen { _ =>
      val h = new JvmHelper
      import h._

      val trace = h.traced {
        assert(!filtered("ok").isDefined)
        p.setValue("ko")
      }

      assert(
        trace == Seq(
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_count", 0),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("jvm/gc_ms", 0),
            None
          )
        )
      )
    }
  }

}
