package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.{CounterType, GaugeType, HistogramType}
import com.twitter.finagle.stats.exp.{Expression, ExpressionSchema, ExpressionSchemaKey}
import org.scalatest.funsuite.AnyFunSuite

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
        statsReceiver.metricBuilder(CounterType).withName(name: _*).withVerbosity(verbosity))
    def addGauge(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    )(
      f: => Float
    ) =
      statsReceiver.addGauge(
        statsReceiver.metricBuilder(GaugeType).withName(name: _*).withVerbosity(verbosity))(f)
    def addHisto(
      statsReceiver: StatsReceiver,
      name: Seq[String],
      verbosity: Verbosity = Verbosity.Default
    ) =
      statsReceiver.stat(
        statsReceiver.metricBuilder(HistogramType).withName(name: _*).withVerbosity(verbosity))
  }
}

class MetricsStatsReceiverTest extends AnyFunSuite {
  import MetricsStatsReceiverTest._

  test("toString") {
    val sr = new MetricsStatsReceiver(Metrics.createDetached())
    assert("MetricsStatsReceiver" == sr.toString)
    assert("MetricsStatsReceiver/s1" == sr.scope("s1").toString)
    assert("MetricsStatsReceiver/s1/s2" == sr.scope("s1").scope("s2").toString)
  }

  def testMetricsStatsReceiver(ctx: TestCtx): Unit = {
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

    test("StatsReceivers share underlying schema maps by default" + suffix) {
      val metrics1 = new Metrics()
      val metrics2 = new Metrics()

      val sr1 = new MetricsStatsReceiver(metrics1)
      val sr2 = new StatsReceiverProxy {
        protected def self: StatsReceiver = new MetricsStatsReceiver(metrics2)
      }

      addCounter(sr1, Seq("aaa"))
      assert(metrics1.schemas.containsKey("aaa"))
      assert(metrics2.schemas.containsKey("aaa"))

      addGauge(sr1, Seq("bbb"))(1f)
      assert(metrics1.schemas.containsKey("bbb"))
      assert(metrics2.schemas.containsKey("bbb"))

      addHisto(sr1, Seq("ccc"))
      assert(metrics1.schemas.containsKey("ccc"))
      assert(metrics2.schemas.containsKey("ccc"))
    }

    test("StatsReceiver metrics expose the underlying schema" + suffix) {
      val metrics = new Metrics()

      val sr = new MetricsStatsReceiver(metrics)
      val counter = addCounter(sr, Seq("aaa"))
      assert(metrics.schemas.get("aaa") == counter.metadata)

      val gauge = addGauge(sr, Seq("bbb"))(1f)
      assert(metrics.schemas.get("bbb") == gauge.metadata)

      val histo = addHisto(sr, Seq("ccc"))
      assert(metrics.schemas.get("ccc") == histo.metadata)
    }

    // scalafix:on StoreGaugesAsMemberVariables
  }

  testMetricsStatsReceiver(new PreSchemaCtx())
  testMetricsStatsReceiver(new SchemaCtx())

  test("expressions are reloaded with fully scoped names") {
    val metrics = Metrics.createDetached()
    val sr = new MetricsStatsReceiver(metrics)

    val aCounter = sr.scope("test").counter("a")
    val bHisto = sr.scope("test").stat("b")
    val cGauge = sr.scope(("test")).addGauge("c") { 1 }

    val expression = ExpressionSchema(
      "test_expression",
      Expression(aCounter.metadata).plus(Expression(bHisto.metadata, Left(Expression.Min))
        .plus(Expression(cGauge.metadata)))
    ).register()

    // what we expected as hydrated metric builders
    val aaSchema =
      MetricBuilder(name = Seq("test", "a"), metricType = CounterType, statsReceiver = sr)
    val bbSchema =
      MetricBuilder(
        name = Seq("test", "b"),
        percentiles = BucketedHistogram.DefaultQuantiles,
        metricType = HistogramType,
        statsReceiver = sr)
    val ccSchema =
      MetricBuilder(name = Seq("test", "c"), metricType = GaugeType, statsReceiver = sr)

    val expected_expression = ExpressionSchema(
      "test_expression",
      Expression(aaSchema).plus(
        Expression(bbSchema, Left(Expression.Min)).plus(Expression(ccSchema))))

    assert(
      metrics.expressions
        .get(ExpressionSchemaKey("test_expression", None, Seq())).expr == expected_expression.expr)
  }

  test(
    "expressions with different serviceNames or different namespaces are stored without clobbering each other") {
    val metrics = Metrics.createDetached()
    val sr = new MetricsStatsReceiver(metrics)
    val exporter = new MetricsExporter(metrics)
    val aCounter =
      MetricBuilder(name = Seq("a"), metricType = CounterType, statsReceiver = sr)

    val expression = ExpressionSchema("test_expression", Expression(aCounter))
      .register()
    assert(exporter.expressions.keySet.size == 1)

    val expressionWithServiceName = ExpressionSchema("test_expression", Expression(aCounter))
      .withServiceName("thrift")
      .register()
    assert(exporter.expressions.keySet.size == 2)

    val expressionWithNamespace = ExpressionSchema("test_expression", Expression(aCounter))
      .withNamespace("a", "b")
      .register()
    assert(exporter.expressions.keySet.size == 3)

    val expressionWithNamespaceAndServiceName =
      ExpressionSchema("test_expression", Expression(aCounter))
        .withNamespace("a", "b").withServiceName("thrift")
        .register()
    assert(exporter.expressions.keySet.size == 4)
  }

}
