package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, Verbosity}
import com.twitter.util.{Await, Duration, Future, Stopwatch, Time}
import org.scalatest.FunSuite
import org.mockito.Mockito.{spy, verify}

class StatsFilterTest extends FunSuite {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response(request)
      response.statusCode = 404
      response.write("hello")
      Future.value(response)
    }
  }

  test("increment stats") {
    val receiver = spy(new InMemoryStatsReceiver)

    val filter = new StatsFilter(receiver, Stopwatch.timeMillis) andThen service

    Time.withCurrentTimeFrozen { _ =>
      Await.result(filter(Request()), Duration.fromSeconds(5))
    }

    assert(receiver.counters(Seq("status", "404")) == 1)
    assert(receiver.counters(Seq("status", "4XX")) == 1)
    assert(receiver.stats(Seq("time", "404")) == Seq(0.0))
    assert(receiver.stats(Seq("time", "4XX")) == Seq(0.0))
  }

  test("status and time counters and stats are memoized") {
    val receiver = spy(new InMemoryStatsReceiver)

    val filter = new StatsFilter(receiver, Stopwatch.timeMillis) andThen service

    Time.withCurrentTimeFrozen { _ =>
      Await.result(filter(Request()), Duration.fromSeconds(5))
      Await.result(filter(Request()), Duration.fromSeconds(5))
    }

    // Verify that the counters and stats were only created once
    verify(receiver).counter(Verbosity.Default, "status", "404")
    verify(receiver).counter(Verbosity.Default, "status", "4XX")
    verify(receiver).stat(Verbosity.Default, "time", "404")
    verify(receiver).stat(Verbosity.Default, "time", "4XX")
  }
}
