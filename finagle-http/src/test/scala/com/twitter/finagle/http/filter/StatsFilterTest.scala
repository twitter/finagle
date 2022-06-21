package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.MetricBuilder
import com.twitter.finagle.stats.NameTranslatingStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Time
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.scalatest.funsuite.AnyFunSuite

class StatsFilterTest extends AnyFunSuite {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response(request)
      response.statusCode = 404
      response.write("hello")
      Future.value(response)
    }
  }

  private class FullTranlationInMemoryStatsReceiver extends InMemoryStatsReceiver {
    override def scopeTranslation = NameTranslatingStatsReceiver.FullTranslation
  }

  test("increment stats") {
    val receiver = new InMemoryStatsReceiver

    val filter = new StatsFilter(receiver, Stopwatch.timeMillis) andThen service

    Time.withCurrentTimeFrozen { _ => Await.result(filter(Request()), Duration.fromSeconds(5)) }

    assert(receiver.counters(Seq("status", "404")) == 1)
    assert(receiver.counters(Seq("status", "4XX")) == 1)
    assert(receiver.stats(Seq("time", "404")) == Seq(0.0))
    assert(receiver.stats(Seq("time", "4XX")) == Seq(0.0))
  }

  test("status and time counters and stats are memoized") {
    val receiver = spy(new FullTranlationInMemoryStatsReceiver)

    val filter = new StatsFilter(receiver, Stopwatch.timeMillis) andThen service

    Time.withCurrentTimeFrozen { _ =>
      Await.result(filter(Request()), Duration.fromSeconds(5))
      Await.result(filter(Request()), Duration.fromSeconds(5))
    }

    // Verify that the counters and stats were only created once
    verify(receiver).counter(MetricBuilder(name = Seq("status", "404"), metricType = CounterType))
    verify(receiver).counter(MetricBuilder(name = Seq("status", "4XX"), metricType = CounterType))
    verify(receiver).stat(MetricBuilder(name = Seq("time", "404"), metricType = HistogramType))
    verify(receiver).stat(MetricBuilder(name = Seq("time", "4XX"), metricType = HistogramType))
  }
}
