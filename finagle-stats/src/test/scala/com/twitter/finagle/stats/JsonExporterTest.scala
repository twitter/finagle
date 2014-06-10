package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import scala.util.matching.Regex
import com.twitter.finagle.http.{Response, Request}
import com.twitter.util.Await

@RunWith(classOf[JUnitRunner])
class JsonExporterTest extends FunSuite {

  test("samples can be filtered") {
    val registry = Metrics.createDetached()
    val exporter = new JsonExporter(registry) {
      override lazy val statsFilterRegex: Option[Regex] = mkRegex("abc,ill_be_partially_matched.*")
    }
    val sample = Map[String, Number](
      "jvm_uptime" -> 15.0,
      "abc" -> 42,
      "ill_be_partially_matched" -> 1
    )
    val filteredSample = exporter.filterSample(sample)
    assert(filteredSample.size == 1, "Expected 1 metric to pass through the filter. Found: " + filteredSample.size)
    assert(filteredSample.contains("jvm_uptime"), "Expected to find jvm_uptime metric in unfiltered samples")
  }

  test("empty regex filter string should not result in a regex") {
    val registry = Metrics.createDetached()
    val exporter = new JsonExporter(registry)
    assert(exporter.mkRegex("").isEmpty, "Empty regex filter should result in no filter regex generated")
  }

  test("end-to-end fetching stats works") {
    val registry = Metrics.createDetached()
    val viewsCounter = registry.createCounter("views")
    val gcCounter = registry.createCounter("jvm_gcs")
    viewsCounter.increment()
    gcCounter.increment()
    val exporter = new JsonExporter(registry) {
      override lazy val statsFilterRegex: Option[Regex] = mkRegex("jvm.*,vie")
    }
    val requestFiltered = Request("/admin/metrics.json?filtered=1&pretty=0")
    val responseFiltered = Response(Await.result(exporter.apply(requestFiltered))).contentString
    assert(responseFiltered.contains("views"), "'Views' should be present - 'vie' is not a match")
    assert(! responseFiltered.contains("jvm_gcs"), "'jvm_gcs' should be present - jvm.* matches it")

    val requestUnfiltered = Request("/admin/metrics.json")
    val responseUnfiltered = Response(Await.result(exporter.apply(requestUnfiltered))).contentString
    assert(responseUnfiltered.contains("views"), "'Views' should be present - 'vie' is not a match")
    assert(responseUnfiltered.contains("jvm_gcs"), "'jvm_gcs' should be present - jvm.* matches it")
  }
}
