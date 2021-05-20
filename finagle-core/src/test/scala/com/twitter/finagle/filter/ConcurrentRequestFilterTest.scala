package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.service.PendingRequestFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Service, ServiceFactory, Stack, StackBuilder, param}
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class ConcurrentRequestFilterTest extends AnyFunSuite {
  val neverFactory = ServiceFactory.const(new Service[Int, Int] {
    def apply(req: Int) = Future.never
  })

  val stk: StackBuilder[ServiceFactory[Int, Int]] = new StackBuilder(
    Stack.leaf(Stack.Role("never"), neverFactory)
  )

  stk.push(ConcurrentRequestFilter.module[Int, Int])

  test("Uses RequestSemaphoreFilter.Param when configured") {
    val sr = new InMemoryStatsReceiver
    val max = 10

    val params = Stack.Params.empty +
      RequestSemaphoreFilter.Param(Some(new AsyncSemaphore(max, 0))) +
      param.Stats(sr)

    val factory = stk.make(params)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)
    assert(sr.gauges(Seq("request_queue_size"))() == 0.0)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)
    assert(sr.gauges(Seq("request_queue_size"))() == 0.0)
  }

  test("Creates PendingRequestFilter when PendingRequestFilter is configured") {
    val sr = new InMemoryStatsReceiver
    val max = 10

    val params = Stack.Params.empty +
      PendingRequestFilter.Param(Some(max)) +
      param.Stats(sr)

    val factory = stk.make(params)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)

    // PendingRequestFilter also has a "rejected" stat
    assert(sr.counters(Seq("rejected")) == 10)
  }

  test("Creates RequestSemaphoreFilter when sem is configured") {
    val sr = new InMemoryStatsReceiver
    val max = 10

    val params = Stack.Params.empty +
      RequestSemaphoreFilter.Param(Some(new AsyncSemaphore(max, 5))) +
      param.Stats(sr)

    val factory = stk.make(params)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)
    assert(sr.gauges(Seq("request_queue_size"))() == 0.0)

    0.until(max).foreach { _ => factory().flatMap(_(1)) }

    assert(sr.gauges(Seq("request_concurrency"))() == max)

    // We can only have a request queue with RequestSemaphoreFilter
    assert(sr.gauges(Seq("request_queue_size"))() == 5.0)
  }
}
