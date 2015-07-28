package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle._
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestSemaphoreFilterTest extends FunSuite {

  test("default config drops the queue tail") {
    val neverFactory = ServiceFactory.const(new Service[Int, Int] {
      def apply(req: Int) = Future.never
    })

    val stk: StackBuilder[ServiceFactory[Int, Int]] = new StackBuilder(
      Stack.Leaf(Stack.Role("never"), neverFactory)
    )

    stk.push(RequestSemaphoreFilter.module[Int, Int])

    val sr = new InMemoryStatsReceiver
    val max = 10

    val params = Stack.Params.empty +
      RequestSemaphoreFilter.Param(Some(new AsyncSemaphore(max, 0))) +
      param.Stats(sr)

    val factory = stk.make(params)

    for (_ <- 0 to max)
      factory().flatMap(_(1))

    assert(sr.gauges(Seq("request_concurrency"))() == max)
    assert(sr.gauges(Seq("request_queue_size"))() == 0.0)

    for (_ <- 0 to max)
      factory().flatMap(_(1))

    assert(sr.gauges(Seq("request_concurrency"))() == max)
    assert(sr.gauges(Seq("request_queue_size"))() == 0.0)
  }

  test("mark dropped requests as rejected") {
    val neverSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.never
    }
    val q = new AsyncSemaphore(1, 0)
    val svc = new RequestSemaphoreFilter(q) andThen neverSvc
    svc(1)
    val f = intercept[Failure] { Await.result(svc(1)) }
    assert(f.isFlagged(Failure.Restartable))
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
}