package com.twitter.finagle.stats

import org.specs.Specification
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time, Timer}

object TimeWindowedStatsRepositorySpec extends Specification {
  "TimeWindowedStatsRepository" should {
    val fakeTimer = new Timer {
      private[this] var f: (() => Unit) = _

      def stop() {}
      def schedule(when: Time, period: Duration)(f: => Unit) = {
        this.f = () => f
        null
      }

      def schedule(when: Time)(f: => Unit) = {
        this.f = () => f
        null
      }

      def tick() { f() }
    }

    val timeWindowedStatsRepository = new TimeWindowedStatsRepository(2, 10.second, fakeTimer)

    "when manipulating current statistics" in {
      "counter" in {
        val counter = timeWindowedStatsRepository.counter("name" -> "foo")
        counter.incr(1)
        counter.incr(1)
        counter.incr(1)
        counter.sum mustEqual 3
      }

      "gauge" in {
        val gauge = timeWindowedStatsRepository.gauge("name" -> "foo")
        gauge.measure(1)
        gauge.measure(2)
        gauge.measure(3)
        gauge.mean mustEqual 2.0f
      }
    }

    "when data rolls off" in {
      "counter" in {
        val counter = timeWindowedStatsRepository.counter("name" -> "foo")
        counter.incr(1)
        counter.incr(1)
        counter.incr(1)
        counter.sum mustEqual 3
        fakeTimer.tick()
        counter.incr(1)
        counter.sum mustEqual 4
        fakeTimer.tick()
        counter.sum mustEqual 1
        fakeTimer.tick()
        counter.sum mustEqual 0
      }

      "gauge" in {
        val gauge = timeWindowedStatsRepository.gauge("name" -> "foo")
        gauge.measure(1)
        gauge.measure(2)
        gauge.measure(3)
        gauge.mean mustEqual 2.0f
        fakeTimer.tick()
        gauge.measure(1)
        gauge.mean mustEqual 1.75f
        fakeTimer.tick()
        gauge.mean mustEqual 1.0f
        fakeTimer.tick()
//        gauge.mean mustEqual NaN
      }
    }
  }
}