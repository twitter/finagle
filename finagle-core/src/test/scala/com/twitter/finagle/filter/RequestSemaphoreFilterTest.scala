package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle._
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.{BufferingTracer, Record, Trace}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class RequestSemaphoreFilterTest extends AnyFunSuite {
  def tracingAnnotations(tracer: BufferingTracer): Seq[(String, Any)] = {
    tracer.iterator.toList collect {
      case Record(_, _, BinaryAnnotation(k, v), _) => k -> v
    }
  }

  test("mark dropped requests as rejected") {
    val neverSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.never
    }
    val q = new AsyncSemaphore(1, 0)
    val svc = new RequestSemaphoreFilter(q) andThen neverSvc
    svc(1)
    val f = intercept[Failure] { Await.result(svc(1)) }
    assert(f.isFlagged(FailureFlags.Retryable))
  }

  test("service failures are not wrapped as rejected") {
    val exc = new Exception("app exc")
    val neverSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.exception(exc)
    }
    val q = new AsyncSemaphore(1, 0)
    val svc = new RequestSemaphoreFilter(q) andThen neverSvc
    svc(1)
    val e = intercept[Exception] { Await.result(svc(1)) }
    assert(e == exc)
  }

  test("annotates dropped requests") {
    val tracer = new BufferingTracer()
    Trace.letTracer(tracer) {
      val neverSvc = new Service[Int, Int] {
        def apply(req: Int) = Future.never
      }
      val q = new AsyncSemaphore(1, 0)
      val svc = new RequestSemaphoreFilter(q) andThen neverSvc
      svc(1)
      val f = intercept[Failure] { Await.result(svc(1)) }
    }

    val expected = Seq(("clnt/RequestSemaphoreFilter_rejected", "Max waiters exceeded"))
    assert(tracingAnnotations(tracer) == expected)
  }
}
