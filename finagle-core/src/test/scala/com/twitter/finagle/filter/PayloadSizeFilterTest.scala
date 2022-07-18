package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funsuite.AnyFunSuite

class PayloadSizeFilterTest extends AnyFunSuite with Eventually with IntegrationPatience {

  private def filter(sr: StatsReceiver) = new PayloadSizeFilter[Int, Int](
    sr,
    "request_bytes",
    "response_bytes",
    req => req, // use the request as the size
    rep => rep // use the response as the size
  )
  private def service(sr: StatsReceiver) = filter(sr).andThen {
    Service.mk[Int, Int] { req => Future.value(req + 1) }
  }

  test("traces sizes when actively tracing") {
    val svc = service(NullStatsReceiver)
    Time.withCurrentTimeFrozen { _ =>
      val tracer = new BufferingTracer
      Trace.letTracer(tracer) {
        assert(Trace.isActivelyTracing)
        assert(6 == Await.result(svc(5)))
      }
      assert(
        tracer.toSeq == Seq(
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("request_bytes", 5),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("response_bytes", 6),
            None
          )
        )
      )
    }
  }

  test("doesn't trace sizes when not actively tracing") {
    val svc = service(NullStatsReceiver)
    val tracer = new BufferingTracer {
      override def isActivelyTracing(traceId: TraceId): Boolean = false
    }
    Trace.letTracer(tracer) {
      assert(!Trace.isActivelyTracing)
      assert(9 == Await.result(svc(8)))
    }
    assert(tracer.toSeq == Nil)
  }

  test("records metrics") {
    val stats = new InMemoryStatsReceiver()
    val svc = service(stats)
    assert(9 == Await.result(svc(8)))
    assert(stats.stat("request_payload_bytes")() == Seq(8f))
    eventually {
      assert(stats.stat("response_payload_bytes")() == Seq(9f))
    }
  }

}
