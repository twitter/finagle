package com.twitter.finagle.filter

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, when}
import org.mockito.Matchers._
import com.twitter.conversions.time._

import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Trace, Record, BufferingTracer, Annotation}
import com.twitter.jvm.{Jvm, Gc}
import com.twitter.util.{TimeControl, Future, Promise, Time}


@RunWith(classOf[JUnitRunner])
class JvmFilterTest extends FunSuite with MockitoSugar {
  class JvmHelper {
    var gcEvents = Nil: List[Gc]
    val jvm = mock[Jvm]
    when(jvm.monitorGcs(any[com.twitter.util.Duration])) thenReturn { since: Time => gcEvents.takeWhile(_.timestamp > since) }
    val mkFilter = new MkJvmFilter(jvm)
    val filter = mkFilter[String, String]()
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    val p = new Promise[String]
    when(service(any[String])) thenReturn p
    val filtered = filter andThen service

    def traced(f: => Unit): Seq[Record] = {
      val b = new BufferingTracer
      Trace.letTracer(b)(f)
      b.toSeq
    }
  }

  test("JvmFilter should Record overlapping Gcs") { tc: TimeControl =>
    val h = new JvmHelper
    import h._

    val trace = traced {
      assert(filtered("ok").poll == None)
      verify(service).apply("ok")
      tc.advance(1.second)
      gcEvents ::= Gc(1, "pcopy", Time.now, 1.second)
      tc.advance(1.second)
      p.setValue("ko")
    }

    assert(trace == Seq(
      Record(
        Trace.id, 1.second.ago,
        Annotation.Message(Gc(1, "pcopy", 1.second.ago, 1.second).toString), Some(1.second))))
  }

  test("JvmFilter should Not record nonoverlapping Gcs") { tc: TimeControl =>
    val h = new JvmHelper
    import h._

    val trace = traced {
      gcEvents ::= Gc(1, "pcopy", Time.now, 1.second)
      tc.advance(10.seconds)
      assert(filtered("ok").poll == None)
      tc.advance(1.second)
      gcEvents ::= Gc(2, "pcopy", Time.now, 1.second)
      p.setValue("ko")
    }

    assert(trace == Seq(
      Record(
        Trace.id, Time.now,
        Annotation.Message(Gc(2, "pcopy", Time.now, 1.second).toString), Some(1.second))))
  }
}
