package com.twitter.finagle.stats

import org.specs.Specification
import com.twitter.conversions.time._

object SimpleStatsRepositorySpec extends Specification {
  "SimpleStatsRepository" should {
    val simpleStatsRepository = new SimpleStatsRepository

    "counter" in {
      val counter = simpleStatsRepository.counter("name" -> "foo")
      counter.incr(1)
      counter.incr(1)
      counter.incr(1)
      counter.sum mustEqual 3
    }

    "gauge" in {
      val gauge = simpleStatsRepository.gauge("name" -> "foo")
      gauge.measure(1)
      gauge.measure(2)
      gauge.measure(3)
      gauge.mean mustEqual 2.0f
    }
  }
}