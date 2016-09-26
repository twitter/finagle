package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito.{spy,verify,times}

@RunWith(classOf[JUnitRunner])
class StatsFilterTest extends FunSuite {

  test("increment stats") {
    val receiver = spy(new InMemoryStatsReceiver)

    val filter = new StatsFilter(receiver) andThen new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        val response = request.response
        response.statusCode = 404
        response.write("hello")
        Future.value(response)
      }
    }

    Time.withCurrentTimeFrozen { _ =>
      Await.result(filter(Request()))
      Await.result(filter(Request()))
    }

    // Verify that the counters and stats were only created once
    verify(receiver, times(1)).counter("status", "404")
    verify(receiver, times(1)).counter("status", "4XX")
    verify(receiver, times(1)).stat("time", "404")
    verify(receiver, times(1)).stat("time", "4XX")
    verify(receiver, times(1)).stat("response_size")

    assert(receiver.counters(Seq("status", "404")) === 2)
    assert(receiver.counters(Seq("status", "4XX")) === 2)
    assert(receiver.stats(Seq("time", "404")) === Seq(0.0, 0.0))
    assert(receiver.stats(Seq("time", "4XX")) === Seq(0.0, 0.0))
    assert(receiver.stats(Seq("response_size")) === Seq(5.0, 5.0))
  }
}
