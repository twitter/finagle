package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.service.NullService
import com.twitter.finagle.stats.{Counter, Gauge, Stat, StatsReceiver}
import com.twitter.util.{Future, Time}
import org.specs.Specification
import scala.collection.mutable


object StatsFilterSpec extends Specification {

  class InMemoryStatsReceiver extends StatsReceiver {
    val counters = mutable.Map[String, Int]()
    val stats    = mutable.Map[String, List[Float]]()
    val gauges   = mutable.Map[String, () => Float]()

    def counter(name: String*): Counter = {
      val fullname = name.mkString(".")
      new Counter {
        def incr(delta: Int) {
          val oldValue = counters.get(fullname).getOrElse(0)
          counters(fullname) = oldValue + delta
        }
      }
    }

    def stat(name: String*): Stat = {
      val fullname = name.mkString(".")
      new Stat {
        def add(value: Float) {
          val oldValue = stats.get(fullname).getOrElse(Nil)
          stats(fullname) = oldValue :+ value
        }
      }
    }

    def addGauge(name: String*)(f: => Float): Gauge = {
      val fullname = name.mkString(",")
      val gauge = new Gauge {
        def remove() {
          gauges -= fullname
        }
      }
      gauges += fullname -> (() => f)
      gauge
    }
  }

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
        filter(Request())()
      }

      receiver.counters("status.404") must_== 1
      receiver.counters("status.4XX") must_== 1
      receiver.stats("time.404")      must_== List(0.0)
      receiver.stats("time.4XX")      must_== List(0.0)
      receiver.stats("response_size") must_== List(5.0)
    }
  }
}
