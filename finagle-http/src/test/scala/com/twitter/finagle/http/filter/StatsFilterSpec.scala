package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future, Time}
import org.specs.SpecificationWithJUnit


class StatsFilterSpec extends SpecificationWithJUnit {
  "StatsFilter" should {
    "increment stats" in {
      val receiver = new InMemoryStatsReceiver

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
      }

      receiver.counters(Seq("status", "404")) must_== 1
      receiver.counters(Seq("status", "4XX")) must_== 1
// TODO: until we can mock stopwatches
//      receiver.stats(Seq("time", "404"))      must_== Seq(0.0)
//      receiver.stats(Seq("time", "4XX"))      must_== Seq(0.0)
      receiver.stats(Seq("response_size"))    must_== Seq(5.0)
    }
  }
}
