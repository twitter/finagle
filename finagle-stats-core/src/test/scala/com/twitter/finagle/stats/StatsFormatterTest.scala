package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class StatsFormatterTest extends AnyFunSuite {

  private[this] def newMetrics(): Metrics =
    Metrics.createDetached(mkHistogram = ImmediateMetricsHistogram.apply _, separator = "/")

  private val metrics = newMetrics()
  private val sr = new MetricsStatsReceiver(metrics)

  private val histo1 = sr.stat("histo1")
  (0 to 100).foreach(histo1.add(_))

  private val values = SampledValues(Seq.empty, Seq.empty, metrics.histograms)

  test("Ostrich") {
    val formatter = StatsFormatter.Ostrich
    val formatted = formatter(values)

    assert(formatted("histo1.p50") == 50)
    assert(formatted("histo1.p90") == 90)
    assert(formatted("histo1.p999") == 100)
    assert(formatted("histo1.p9999") == 100)

    assert(formatted("histo1.count") == 101)
    assert(formatted("histo1.maximum") == 100)
    assert(formatted("histo1.minimum") == 0)
    assert(formatted("histo1.average") == 50)
  }

  test("Metrics") {
    val formatter = StatsFormatter.CommonsMetrics
    val formatted = formatter(values)

    assert(formatted("histo1.p50") == 50)
    assert(formatted("histo1.p90") == 90)
    assert(formatted("histo1.p9990") == 100)
    assert(formatted("histo1.p9999") == 100)

    assert(formatted("histo1.count") == 101)
    assert(formatted("histo1.max") == 100)
    assert(formatted("histo1.min") == 0)
    assert(formatted("histo1.avg") == 50)
  }

  test("CommonsStats") {
    val formatter = StatsFormatter.CommonsStats
    val formatted = formatter(values)

    assert(formatted("histo1_50_0_percentile") == 50)
    assert(formatted("histo1_90_0_percentile") == 90)
    assert(formatted("histo1_99_0_percentile") == 99)
    assert(formatted("histo1_99_9_percentile") == 100)
    assert(formatted("histo1_99_99_percentile") == 100)

    assert(formatted("histo1_count") == 101)
    assert(formatted("histo1_max") == 100)
    assert(formatted("histo1_min") == 0)
    assert(formatted("histo1_avg") == 50)
  }

  test("includeEmptyHistograms flag") {
    val metrics = newMetrics()
    val stats = new MetricsStatsReceiver(metrics)
    stats.stat("empty_histo")
    val values = SampledValues(Seq.empty, Seq.empty, metrics.histograms)

    val formatter = StatsFormatter.Ostrich
    includeEmptyHistograms.let(false) {
      val formatted = formatter(values)
      assert(Map("empty_histo.count" -> 0) == formatted)
    }
  }

  test("name collisions are allowed") {
    val metrics = newMetrics()
    val stats = new MetricsStatsReceiver(metrics)

    stats.counter("a", "a")
    stats.counter("a", "a") // doesn't throw
    stats.counter("a/a") // doesn't throw
  }

}
