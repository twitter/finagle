package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Trace, Record, BufferingTracer, Annotation}
import com.twitter.jvm.{Jvm, Gc}
import com.twitter.util.{Future, Promise, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class JvmFilterSpec extends SpecificationWithJUnit with Mockito {
  "JvmFilter" should {
    var gcEvents = Nil : List[Gc]
    val jvm = mock[Jvm]
    jvm.monitorGcs(any) returns { since => gcEvents.takeWhile(_.timestamp > since) }
    val mkFilter = new MkJvmFilter(jvm)
    val filter = mkFilter[String, String]()
    val service = mock[Service[String, String]]
    service.close(any) returns Future.Done
    val p = new Promise[String]
    service(any) returns p
    val filtered = filter andThen service

    def traced(f: => Unit): Seq[Record] = Trace.unwind {
      val b = new BufferingTracer
      Trace.pushTracer(b)
      f
      b.toSeq
    }

    "Record overlapping Gcs" in Time.withCurrentTimeFrozen { tc =>
      val trace = traced {
        filtered("ok").poll must beNone
        there was one(service).apply("ok")
        tc.advance(1.second)
        gcEvents ::= Gc(1, "pcopy", Time.now, 1.second)
        tc.advance(1.second)
        p.setValue("ko")
      }

      trace must be_==(Seq(
        Record(
          Trace.id, 1.second.ago,
          Annotation.Message(Gc(1, "pcopy", 1.second.ago, 1.second).toString), Some(1.second))))
    }

    "Not record nonoverlapping Gcs" in Time.withCurrentTimeFrozen { tc =>
      val trace = traced {
        gcEvents ::= Gc(1, "pcopy", Time.now, 1.second)
        tc.advance(10.seconds)
        filtered("ok").poll must beNone
        tc.advance(1.second)
        gcEvents ::= Gc(2, "pcopy", Time.now, 1.second)
        p.setValue("ko")
      }

      trace must be_==(Seq(
        Record(
          Trace.id, Time.now,
          Annotation.Message(Gc(2, "pcopy", Time.now, 1.second).toString), Some(1.second))))
    }
  }
}
