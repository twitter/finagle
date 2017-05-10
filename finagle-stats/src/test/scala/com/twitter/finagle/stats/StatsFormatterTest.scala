package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class StatsFormatterTest extends FunSuite {

  private val metrics = Metrics.createDetached()
  private val sr = new ImmediateMetricsStatsReceiver(metrics)

  private val histo1 = sr.stat("histo1")
  (0 to 100).foreach(histo1.add(_))

  private val values = SampledValues(
    Map.empty,
    Map.empty,
    metrics.sampleHistograms().asScala)

  test("CommonsMetrics is formatted the same as Metrics.sample") {
    val formatter = StatsFormatter.CommonsMetrics
    val formatted = formatter(values)

    // remove stddev as it is not supported
    assert(formatted == metrics.sample().asScala.filterKeys(!_.endsWith("stddev")))

    assert(formatted("histo1.p50") == 50)
    assert(formatted("histo1.p90") == 90)
    assert(formatted("histo1.p9990") == 100)
    assert(formatted("histo1.p9999") == 100)

    assert(formatted("histo1.count") == 101)
    assert(formatted("histo1.max") == 100)
    assert(formatted("histo1.min") == 0)
    assert(formatted("histo1.avg") == 50)
  }

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
    val metrics = Metrics.createDetached()
    val stats = new ImmediateMetricsStatsReceiver(metrics)
    stats.stat("empty_histo")
    val values = SampledValues(
      Map.empty,
      Map.empty,
      metrics.sampleHistograms().asScala)

    val formatter = StatsFormatter.Ostrich
    includeEmptyHistograms.let(false) {
      val formatted = formatter(values)
      assert(Map("empty_histo.count" -> 0) == formatted)
    }
  }

}
