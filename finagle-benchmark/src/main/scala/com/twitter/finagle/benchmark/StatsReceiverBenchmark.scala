package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.stats.{Stat, MetricsStatsReceiver, OstrichStatsReceiver}
import java.util.Random
import com.twitter.common.metrics.Metrics

// From $BIRDCAGE_HOME run:
// ./pants goal bench finagle/finagle-benchmark --bench-target=com.twitter.finagle.benchmark.StatsReceiverBenchmark
class StatsReceiverBenchmark extends SimpleBenchmark {

  private[this] val rnd = new Random
  private[this] var ostrich: OstrichStatsReceiver = null
  private[this] var metrics: MetricsStatsReceiver = null
  private[this] var metricsStat: Stat = null
  private[this] var ostrichStat: Stat = null

  protected override def setUp() {
    ostrich = new OstrichStatsReceiver()
    metrics = new MetricsStatsReceiver(Metrics.createDetached())
    metricsStat = metrics.stat("histo")
    ostrichStat = ostrich.stat("histo")
    (1 to 100) foreach { _ =>
      val oStat = ostrich.stat("my_stat")
      val mStat = metrics.stat("my_stat")
      (1 to 1000) foreach { x =>
        val rand = rnd.nextInt(x)
        oStat.add(rand)
        mStat.add(rand)
      }
    }
  }

  protected override def tearDown() {
    ostrich.repr.clearAll()
  }

  def timeOstrichStatsCreation(n: Int) {
    var i = 0
    while (i < n) {
      ostrich.stat("ostrich_histogram")
      i += 1
    }
  }

  def timeMetricsStatsCreation(n: Int) {
    var i = 0
    while (i < n) {
      metrics.stat("metrics_histogram")
      i += 1
    }
  }

  def timeOstrichStatsInsertion(n: Int) {
    var i = 0
    while (i < n) {
      ostrichStat.add(i.toFloat)
      i += 1
    }
  }

  def timeMetricsStatsInsertion(n: Int) {
    var i = 0
    while (i < n) {
      metricsStat.add(i.toFloat)
      i += 1
    }
  }

  def timeOstrichStatsQuery(n: Int) {
    var i = 0
    while (i < n) {
      ostrich.repr.get()
      i += 1
    }
  }

  def timeMetricsStatsQuery(n: Int) {
    var i = 0
    while (i < n) {
      metrics.registry.sample()
      i += 1
    }
  }
}
