package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Request
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.util.Await
import com.twitter.util.Awaitable
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.funsuite.AnyFunSuite

class PrometheusExporterHandlerTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("close() logs metrics data when logOnShutdown is enabled") {
    logOnShutdown.let(true) {
      val logSeen = new AtomicBoolean()
      val exporter = new PrometheusExporterHandler(new TestMetricsView(Seq(), Seq(), Seq())) {
        override protected def doLog(): Unit = {
          logSeen.set(true)
        }
      }
      val f = exporter.close()

      await(f)
      assert(logSeen.get)
    }
  }

  test("close() does not log metric data when logOnShutdown is disabled") {
    logOnShutdown.let(false) {
      val logSeen = new AtomicBoolean()
      val exporter = new PrometheusExporterHandler(new TestMetricsView(Seq(), Seq(), Seq())) {
        override protected def doLog(): Unit = {
          logSeen.set(true)
        }
      }
      val f = exporter.close()

      await(f)
      assert(!logSeen.get)
    }
  }

  test("Can handle the 'export_metadata' query param") {
    val exporter = new PrometheusExporterHandler(
      new TestMetricsView(
        Seq.empty,
        Seq.empty,
        Seq(SampleSnapshots.EmptyHistoSample)
      ))

    // Defaults to hiding the metadata
    assert(
      !await(exporter(Request("/"))).contentString
        .contains("# TYPE"))

    assert(
      await(exporter(Request("/?export_metadata=true"))).contentString
        .contains("# TYPE"))

    assert(
      !await(exporter(Request("/?export_metadata=false"))).contentString
        .contains("# TYPE"))
  }

  test("Can handle the 'export_empty_quantiles' query param") {
    val exporter = new PrometheusExporterHandler(
      new TestMetricsView(
        Seq.empty,
        Seq.empty,
        Seq(SampleSnapshots.EmptyHistoSample)
      ))

    // Defaults to using the `includeEmptyHistograms` flag if query param isn't present
    assert(
      !await(exporter(Request("/"))).contentString
        .contains("quantile="))

    includeEmptyHistograms.let(true) {
      assert(
        await(exporter(Request("/"))).contentString
          .contains("quantile="))
    }

    // Prefers the query param over the flag
    includeEmptyHistograms.let(false) {
      assert(
        await(exporter(Request("/?export_empty_quantiles=true"))).contentString
          .contains("quantile="))
    }

    assert(
      await(exporter(Request("/?export_empty_quantiles=true"))).contentString
        .contains("quantile="))

    assert(
      !await(exporter(Request("/?export_empty_quantiles=false"))).contentString
        .contains("quantile="))
  }

  test("Can handle the 'verbosity_pattern' query parameter") {
    val debugCounter =
      CounterSnapshot(
        hierarchicalName = "debug_requests",
        builder = MetricBuilder(
          name = Seq("debug_requests"),
          metricType = CounterType,
          units = Requests,
          verbosity = Verbosity.Debug
        ).withDimensionalSupport,
        value = 1
      )

    val exporter = new PrometheusExporterHandler(
      new TestMetricsView(
        Seq(debugCounter),
        Seq.empty,
        Seq.empty
      ))

    // Defaults to not emitting debug metrics
    assert(await(exporter(Request("/"))).contentString.isEmpty)

    // Will use the flag if no query param is present
    verbose.let("debug*") {
      assert(
        await(exporter(Request("/"))).contentString
          .contains("debug_requests"))
    }

    // Prefers the query param over the flag
    verbose.let("foo") {
      assert(
        await(exporter(Request("/?verbosity_pattern=debug_requests"))).contentString
          .contains("debug_requests"))
    }

    assert(
      await(exporter(Request("/?verbosity_pattern=debug_requests"))).contentString
        .contains("debug_requests"))

    assert(
      await(exporter(Request("/?verbosity_pattern=debug*"))).contentString
        .contains("debug_requests"))
  }
}
