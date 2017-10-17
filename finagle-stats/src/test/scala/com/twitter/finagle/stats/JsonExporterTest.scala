package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.finagle.http.{MediaType, Request, RequestParamMap}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.tunable.Tunable
import com.twitter.util.{Await, MockTimer, Time}
import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import scala.util.matching.Regex

@RunWith(classOf[JUnitRunner])
class JsonExporterTest extends FunSuite with Eventually with IntegrationPatience {

  import JsonExporter._

  // 2015-02-05 20:05:00 +0000
  private val zeroSecs = Time.fromSeconds(1423166700)

  test("readBooleanParam") {
    val exporter = new JsonExporter(Metrics.createDetached())
    val r = Request()

    def assertParam(r: Request, expected: Boolean, default: Boolean): Unit =
      withClue(s"params=${r.params}") {
        assert(expected == exporter.readBooleanParam(new RequestParamMap(r), "hi", default))
      }

    // param doesn't exist so uses default
    assertParam(Request(), expected = false, default = false)
    assertParam(Request(), expected = true, default = true)

    // param exists but value not true, so always false
    assertParam(Request(("hi", "")), expected = false, default = false)
    assertParam(Request(("hi", "")), expected = false, default = true)
    assertParam(Request(("hi", ""), ("hi", "nope")), expected = false, default = true)

    // param exists and value is true, so always true
    assertParam(Request(("hi", "1")), expected = true, default = false)
    assertParam(Request(("hi", "true")), expected = true, default = true)
    assertParam(Request(("hi", "no"), ("hi", "true")), expected = true, default = true)
  }

  test("samples can be filtered") {
    val registry = Metrics.createDetached()
    val exporter = new JsonExporter(registry) {
      override lazy val statsFilterRegex: Option[Regex] =
        commaSeparatedRegex("abc,ill_be_partially_matched.*")
    }
    val sample = Map[String, Number](
      "jvm_uptime" -> 15.0,
      "abc" -> 42,
      "ill_be_partially_matched" -> 1
    )
    val filteredSample = exporter.filterSample(sample)
    assert(
      filteredSample.size == 1,
      "Expected 1 metric to pass through the filter. Found: " + filteredSample.size
    )
    assert(
      filteredSample.contains("jvm_uptime"),
      "Expected to find jvm_uptime metric in unfiltered samples"
    )
  }

  test("empty regex filter string should not result in a regex") {
    val registry = Metrics.createDetached()
    val exporter = new JsonExporter(registry)
    assert(
      commaSeparatedRegex("").isEmpty,
      "Empty regex filter should result in no filter regex generated"
    )
  }

  test("statsFilterFile defaults without exception") {
    val registry = Metrics.createDetached()
    val exporter1 = new JsonExporter(registry)
    assert(exporter1.statsFilterRegex.isEmpty)
  }

  test("statsFilterFile reads empty files") {
    val registry = Metrics.createDetached()

    statsFilterFile.let(Set(new File("/dev/null"))) {
      val exporter = new JsonExporter(registry)
      assert(exporter.statsFilterRegex.isEmpty)
    }
  }

  test("statsFilterFile reads multiple files") {
    val registry = Metrics.createDetached()

    val tFile1 = File.createTempFile("regex", ".txt")
    tFile1.deleteOnExit()

    val writer1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tFile1), "UTF-8"))
    writer1.write("abc123\r\n")
    writer1.close()

    val tFile2 = File.createTempFile("regex", ".txt")
    tFile2.deleteOnExit()

    val writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tFile2), "UTF-8"))
    writer2.write("def456\r\n")
    writer2.write("ghi789\r\n")
    writer2.close()

    statsFilterFile.let(Set(tFile1, tFile2)) {
      val exporter = new JsonExporter(registry)
      val regex = exporter.statsFilterRegex
      assert(regex.isDefined)
      assert(regex.get.pattern.matcher("abc123").matches)
      assert(regex.get.pattern.matcher("def456").matches)
      assert(regex.get.pattern.matcher("ghi789").matches)
    }
  }

  test("statsFilterFile and statsFilter combine") {
    val registry = Metrics.createDetached()

    val tFile = File.createTempFile("regex", ".txt")
    tFile.deleteOnExit()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tFile), "UTF-8"))
    writer.write("abc123\r\n")
    writer.close()

    statsFilterFile.let(Set(tFile)) {
      statsFilter.let("def456") {
        val exporter = new JsonExporter(registry)
        val regex = exporter.statsFilterRegex
        assert(regex.isDefined)
        assert(regex.get.pattern.matcher("abc123").matches)
        assert(regex.get.pattern.matcher("def456").matches)
      }
    }
  }

  test("end-to-end fetching stats works") {
    val registry = Metrics.createDetached()
    val viewsCounter = registry.getOrCreateCounter(Verbosity.Default, Seq("views")).counter
    val gcCounter = registry.getOrCreateCounter(Verbosity.Default, Seq("jvm_gcs")).counter
    viewsCounter.incr()
    gcCounter.incr()
    val exporter = new JsonExporter(registry) {
      override lazy val statsFilterRegex: Option[Regex] = commaSeparatedRegex("jvm.*,vie")
    }
    val requestFiltered = Request("/admin/metrics.json?filtered=1&pretty=0")
    val responseFiltered = Await.result(exporter.apply(requestFiltered)).contentString
    assert(responseFiltered.contains("views"), "'Views' should be present - 'vie' is not a match")
    assert(!responseFiltered.contains("jvm_gcs"), "'jvm_gcs' should be present - jvm.* matches it")

    val requestUnfiltered = Request("/admin/metrics.json")
    val responseUnfiltered = Await.result(exporter.apply(requestUnfiltered))
    assert(Some(MediaType.Json) == responseUnfiltered.contentType)

    val responseUnfilteredContent = responseUnfiltered.contentString
    assert(
      responseUnfilteredContent.contains("views"),
      "'Views' should be present - 'vie' is not a match"
    )
    assert(
      responseUnfilteredContent.contains("jvm_gcs"),
      "'jvm_gcs' should be present - jvm.* matches it"
    )
  }

  test("startOfNextMinute") {
    Time.withTimeAt(zeroSecs) { tc =>
      assert(JsonExporter.startOfNextMinute == zeroSecs + 1.minute)

      tc.advance(1.second) // 01 second past the minute
      assert(JsonExporter.startOfNextMinute == zeroSecs + 1.minute)

      tc.advance(58.seconds) // 59 seconds past the minute
      assert(JsonExporter.startOfNextMinute == zeroSecs + 1.minute)

      tc.advance(1.second) // 60 seconds past the minute
      assert(JsonExporter.startOfNextMinute == zeroSecs + 2.minutes)
    }
  }

  test("useCounterDeltas flag enabled") {
    val reqWithPeriod = Request("/admin/metrics.json?period=60")
    val reqNoPeriod = Request("/admin/metrics.json")

    val name = "anCounter"
    val registry = Metrics.createDetached()
    val counter = registry.getOrCreateCounter(Verbosity.Default, Seq(name)).counter

    val timer = new MockTimer()
    val exporter = new JsonExporter(registry, timer)

    useCounterDeltas.let(true) {
      // start in the past so we are guaranteed a run immediately
      Time.withTimeAt(zeroSecs) { control =>
        def update() = {
          control.advance(61.seconds)
          timer.tick()
        }

        // we won't trigger an `update` until the first minute.
        val emptyRes = Await.result(exporter(reqWithPeriod)).contentString
        assert(emptyRes == "{}")

        update()
        eventually {
          val res = Await.result(exporter(reqWithPeriod)).contentString
          assert(res == """{"anCounter":0}""")
        }

        // Note: the `CounterDeltas.update()`s happen async
        counter.incr(11)
        update()
        eventually {
          // with the param
          val res = Await.result(exporter(reqWithPeriod)).contentString
          assert(res == """{"anCounter":11}""")
        }

        counter.incr(5)
        update()
        eventually {
          // verify returning deltas, when param requested
          val res = Await.result(exporter(reqWithPeriod)).contentString
          assert(res == """{"anCounter":5}""")
        }

        // verify totals returned when param omitted
        val res3 = Await.result(exporter(reqNoPeriod)).contentString
        assert(res3 == """{"anCounter":16}""")
        counter.incr(5)
        update() // should not matter when the param is omitted.
        val res4 = Await.result(exporter(reqNoPeriod)).contentString
        assert(res4 == """{"anCounter":21}""")
      }
    }
  }

  test("useCounterDeltas flag disabled") {
    val reqWithPeriod = Request("/admin/metrics.json?period=60")

    val registry = Metrics.createDetached()
    val counter = registry.getOrCreateCounter(Verbosity.Default, Seq("anCounter")).counter
    counter.incr(11)

    val timer = new MockTimer()
    val exporter = new JsonExporter(registry, timer)

    useCounterDeltas.let(false) {
      Time.withCurrentTimeFrozen { control =>
        def update() = {
          control.advance(61.seconds)
          timer.tick()
        }

        // with counterDeltas param
        val res1 = Await.result(exporter(reqWithPeriod)).contentString
        assert(res1 == """{"anCounter":11}""")

        // update should have no effect, even when param included
        counter.incr(5)
        update()
        val res2 = Await.result(exporter(reqWithPeriod)).contentString
        assert(res2 == """{"anCounter":16}""")
      }
    }
  }

  test("formatter flag") {
    val registry = Metrics.createDetached(
      mkHistogram = ImmediateMetricsHistogram.apply _, separator = "/")
    val sr = new MetricsStatsReceiver(registry)
    val histo = sr.stat("anHisto")
    histo.add(555)

    val req = Request("/admin/metrics.json")

    format.let(format.Ostrich) {
      val exporter = new JsonExporter(registry)
      val res = Await.result(exporter(req)).contentString
      assert(res.contains(""""anHisto.maximum":558"""))
    }

    format.let(format.CommonsMetrics) {
      val exporter = new JsonExporter(registry)
      val res = Await.result(exporter(req)).contentString
      assert(res.contains(""""anHisto.max":558"""))
    }
  }

  test("deadly gauge") {
    val registry = Metrics.createDetached()
    val sr =
      registry.registerGauge(Verbosity.Default, Seq("boom"), throw new RuntimeException("loolool"))

    val exporter = new JsonExporter(registry)
    val json = exporter.json(pretty = true, filtered = false)
    assert(!json.contains("boom"), json)
  }

  test("respecting verbosity levels") {
    val metrics = Metrics.createDetached()
    val sr = new MetricsStatsReceiver(metrics)
    sr.counter(Verbosity.Debug, "foo", "bar").incr(10)
    sr.counter(Verbosity.Debug, "foo", "baz").incr(20)
    sr.counter(Verbosity.Debug, "qux").incr(30)
    sr.counter(Verbosity.Default, "aux").incr(40)

    val exporter =
      new JsonExporter(
        metrics,
        Tunable.const(Verbose.id, "*/bar,qux"),
        DefaultTimer
      )

    val json = exporter.json(pretty = false, filtered = false)
    assert(json.contains(""""foo/bar":10"""))
    assert(json.contains(""""qux":30"""))
    assert(json.contains(""""aux":40"""))
    assert(!json.contains(""""foo/baz":20"""))
  }

  test("escape regexp special characters in glob") {
    assert(
      commaSeparatedGlob("[]^$a.|?b*+()c{}d").forall(_.matcher("[]^$a.|?bcde*+()c{}d").matches)
    )
  }
}
