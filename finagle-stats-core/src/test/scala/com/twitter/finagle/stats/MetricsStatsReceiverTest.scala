package com.twitter.finagle.stats

import org.scalatest.FunSuite

object MetricsStatsReceiverTest {
  trait TestCtx {
    val suffix = "default"
    def addCounter(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ): Counter
    def addGauge(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    )(
      f: => Float
    ): Gauge
    def addHisto(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ): Stat

    // rootReceiver needs to be isolated to each subclass.
    val rootReceiver = new MetricsStatsReceiver(Metrics.createDetached())

    def readGauge(metrics: MetricsStatsReceiver, name: String): Number =
      metrics.registry.gauges.get(name)

    def readGaugeInRoot(name: String) = readGauge(rootReceiver, name)
    def readCounterInRoot(name: String) = rootReceiver.registry.counters.get(name)
  }

  class PreSchemaCtx extends TestCtx {
    override val suffix = " with pre-schema methods"

    def addCounter(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ) = statsReceiver.counter(verbosity, name: _*)
    def addGauge(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    )(
      f: => Float
    ) = statsReceiver.addGauge(verbosity, name: _*)(f)
    def addHisto(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ) = statsReceiver.stat(verbosity, name: _*)
  }

  class SchemaCtx extends TestCtx {
    override val suffix = " with schema methods"

    def addCounter(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ) =
      statsReceiver.counter(
        CounterSchema(statsReceiver.metricBuilder().withName(name).withVerbosity(verbosity)))
    def addGauge(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    )(
      f: => Float
    ) =
      statsReceiver.addGauge(
        GaugeSchema(statsReceiver.metricBuilder().withName(name).withVerbosity(verbosity)))(f)
    def addHisto(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ) =
      statsReceiver.stat(
        HistogramSchema(statsReceiver.metricBuilder().withName(name).withVerbosity(verbosity)))
  }
}

class MetricsStatsReceiverTest extends FunSuite {
  import MetricsStatsReceiverTest._

  test("toString") {
    val sr = new MetricsStatsReceiver(Metrics.createDetached())
    assert("MetricsStatsReceiver" == sr.toString)
    assert("MetricsStatsReceiver/s1" == sr.scope("s1").toString)
    assert("MetricsStatsReceiver/s1/s2" == sr.scope("s1").scope("s2").toString)
  }

  def testMetricsStatsReceiver(ctx: TestCtx) {
    import ctx._

    // scalafix:off StoreGaugesAsMemberVariables
    test("MetricsStatsReceiver should store and read gauge into the root StatsReceiver" + suffix) {
      val x = 1.5f
      // gauges are weakly referenced by the registry so we need to keep a strong reference
      val g = addGauge(rootReceiver, Seq("my_gauge"))(x)
      assert(readGaugeInRoot("my_gauge") == x)
    }

    test("cumulative gauge is working" + suffix) {
      val x = 1
      val y = 2
      val z = 3
      val g1 = addGauge(rootReceiver, Seq("my_cumulative_gauge"))(x)
      val g2 = addGauge(rootReceiver, Seq("my_cumulative_gauge"))(y)
      val g3 = addGauge(rootReceiver, Seq("my_cumulative_gauge"))(z)
      assert(readGaugeInRoot("my_cumulative_gauge") == x + y + z)
    }

    test(
      "Ensure that we throw an exception with a counter and a gauge when rollup collides" + suffix) {
      val sr = new RollupStatsReceiver(rootReceiver)
      addCounter(sr, Seq("a", "b", "c")).incr()
      intercept[MetricCollisionException] {
        sr.addGauge("a", "b", "d") {
          3
        }
      }
    }

    test("Ensure that we throw an exception when rollup collides via scoping" + suffix) {
      val sr = new RollupStatsReceiver(rootReceiver)
      val newSr = sr.scope("a").scope("b")
      addCounter(newSr, Seq("c")).incr()
      intercept[MetricCollisionException] {
        addGauge(newSr, Seq("d")) {
          3
        }
      }
    }

    test("reading histograms initializes correctly" + suffix) {
      val sr = new MetricsStatsReceiver(Metrics.createDetached())
      val stat = addHisto(sr, Seq("my_cool_stat"))

      val reader = sr.registry.histoDetails.get("my_cool_stat")
      assert(reader != null && reader.counts == Nil)

      // also ensure the buckets were populated with default values.
      assert(sr.registry.histograms.get("my_cool_stat").percentiles.length == 6)
    }

    test("store and read counter into the root StatsReceiver" + suffix) {
      addCounter(rootReceiver, Seq("my_counter")).incr()
      assert(readCounterInRoot("my_counter") == 1)
    }

    test("separate gauge/stat/metric between detached Metrics and root Metrics" + suffix) {
      val detachedReceiver = new MetricsStatsReceiver(Metrics.createDetached())
      val g1 = addGauge(detachedReceiver, Seq("xxx"))(1.0f)
      val g2 = addGauge(rootReceiver, Seq("xxx"))(2.0f)
      assert(readGauge(detachedReceiver, "xxx") != readGauge(rootReceiver, "xxx"))
    }

    test("keep track of debug metrics" + suffix) {
      val metrics = Metrics.createDetached()
      val sr = new MetricsStatsReceiver(metrics)

      addCounter(sr, Seq("foo"), Verbosity.Debug)
      addHisto(sr, Seq("bar"), Verbosity.Debug)
      addGauge(sr, Seq("baz"), Verbosity.Debug)(0f)

      assert(metrics.verbosity.get("foo") == Verbosity.Debug)
      assert(metrics.verbosity.get("bar") == Verbosity.Debug)
      assert(metrics.verbosity.get("baz") == Verbosity.Debug)
    }

    test("does not keep track of default metrics" + suffix) {
      val metrics = Metrics.createDetached()
      val sr = new MetricsStatsReceiver(metrics)

      addCounter(sr, Seq("foo"), Verbosity.Default)
      addHisto(sr, Seq("bar"), Verbosity.Default)
      addGauge(sr, Seq("baz"), Verbosity.Default)(0f)

      assert(!metrics.verbosity.containsKey("foo"))
      assert(!metrics.verbosity.containsKey("bar"))
      assert(!metrics.verbosity.containsKey("baz"))
    }

    test("only assign verbosity at creation" + suffix) {
      val metrics = Metrics.createDetached()
      val sr = new MetricsStatsReceiver(metrics)

      addCounter(sr, Seq("foo"), Verbosity.Default)
      addHisto(sr, Seq("bar"), Verbosity.Default)
      addGauge(sr, Seq("baz"), Verbosity.Default)(0f)

      addCounter(sr, Seq("foo"), Verbosity.Debug)
      addHisto(sr, Seq("bar"), Verbosity.Debug)
      addGauge(sr, Seq("baz"), Verbosity.Debug)(0f)

      assert(!metrics.verbosity.containsKey("foo"))
      assert(!metrics.verbosity.containsKey("bar"))
      assert(!metrics.verbosity.containsKey("baz"))
    }

    test("StatsReceivers share underlying metrics maps by default" + suffix) {
      val metrics1 = new Metrics()
      val metrics2 = new Metrics()

      val sr1 = new MetricsStatsReceiver(metrics1)
      val sr2 = new MetricsStatsReceiver(metrics2)

      addCounter(sr1, Seq("foo"))
      assert(metrics1.counters.containsKey("foo"))
      assert(metrics2.counters.containsKey("foo"))

      addGauge(sr1, Seq("bar"))(1f)
      assert(metrics1.gauges.containsKey("bar"))
      assert(metrics2.gauges.containsKey("bar"))

      addHisto(sr1, Seq("baz"))
      assert(metrics1.histograms.containsKey("baz"))
      assert(metrics2.histograms.containsKey("baz"))
    }

    // scalafix:on StoreGaugesAsMemberVariables
  }

  testMetricsStatsReceiver(new PreSchemaCtx())
  testMetricsStatsReceiver(new SchemaCtx())
}
