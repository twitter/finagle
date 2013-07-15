package com.twitter.finagle.stats

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.common.metrics.Metrics


class MetricsStatsReceiverSpec extends SpecificationWithJUnit with Mockito {
  "MetricsStatsReceiver stats system" should {
    val rootReceiver = new MetricsStatsReceiver()

    def read(metrics: MetricsStatsReceiver)(name: String): Number =
      metrics.registry.sample().get(name)

    def readInRoot(name: String) = read(rootReceiver)(name)

    "store and read gauge into the root StatsReceiver" in {
      val x = 1.5f
      rootReceiver.addGauge("my_gauge")(x)
      readInRoot("my_gauge") must be_==(x)
    }

    "store and read counter into the root StatsReceiver" in {
      rootReceiver.counter("my_counter").incr()
      readInRoot("my_counter") must be_==(1)
    }

    "separate gauge/stat/metric between detached Metrics and root Metrics" in {
      val detachedReceiver = new MetricsStatsReceiver(Metrics.createDetached())
      detachedReceiver.addGauge("xxx")(1.0f)
      rootReceiver.addGauge("xxx")(2.0f)
      read(detachedReceiver)("xxx") mustNotEq read(rootReceiver)("xxx")
    }
  }
}
