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

}
