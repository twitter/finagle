package com.twitter.finagle
package service

import com.twitter.finagle.stats.CategorizingExceptionStatsHandler
import com.twitter.finagle.stats.ExceptionStatsHandler
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.stats.exp.FunctionExpression
import com.twitter.finagle.stats.exp.HistogramExpression
import com.twitter.finagle.stats.exp.MetricExpression
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.TimeUnit
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class BasicServiceMetricsTest extends AnyFunSuite with OneInstancePerTest {

  // Some magic numbers signal failure that otherwise would not so we can test that it's used.
  private[this] val responseClassifier: ResponseClassifier = {
    case ReqRep(-1, _) => ResponseClass.RetryableFailure
    case ReqRep(_, Return(-1)) => ResponseClass.NonRetryableFailure
  }

  private[this] val BasicExceptions =
    new CategorizingExceptionStatsHandler(_ => None, _ => None, rollup = false)

  private[this] val timeUnit = TimeUnit.MILLISECONDS

  private[this] val statsReceiver = new InMemoryStatsReceiver()
  private[this] val metricBuilderRegistry = new CoreMetricsRegistry()

  private[this] def serviceMetrics(
    timeUnit: TimeUnit = timeUnit,
    exceptionStatsHandler: ExceptionStatsHandler = BasicExceptions,
    metricsRegistry: Option[CoreMetricsRegistry] = Some(metricBuilderRegistry),
    statsReceiver: StatsReceiver = statsReceiver,
    responseClassifier: ResponseClassifier = responseClassifier
  ): BasicServiceMetrics = new BasicServiceMetrics(
    timeUnit = timeUnit,
    exceptionStatsHandler = exceptionStatsHandler,
    metricsRegistry = metricsRegistry,
    statsReceiver = statsReceiver,
    responseClassifier = responseClassifier
  )

  private[this] val basicServiceMetrics = serviceMetrics()

  private[this] def nameToKey(
    name: String,
    labels: Map[String, String] = Map()
  ): ExpressionSchemaKey =
    ExpressionSchemaKey(name, labels, Seq())

  test("instantiates metrics") {
    assert(statsReceiver.counters.get(Seq("success")).isDefined)
    assert(statsReceiver.counters.get(Seq("failures")).isDefined)
    assert(statsReceiver.counters.get(Seq("requests")).isDefined)
    assert(statsReceiver.stats.get(Seq("request_latency_ms")).isDefined)
  }

  test("expressions are instrumented if we provide the core metric registry") {
    assert(
      statsReceiver.expressions(nameToKey("success_rate")).expr.isInstanceOf[FunctionExpression])
    assert(statsReceiver.expressions(nameToKey("throughput")).expr.isInstanceOf[MetricExpression])
    assert(
      statsReceiver
        .expressions(nameToKey("latency", Map("bucket" -> "p99"))).expr.isInstanceOf[
          HistogramExpression])
  }

  test("expressions are not instrumented if we don't provide the core metric registry") {
    val statsReceiver = new InMemoryStatsReceiver()
    val basicServiceMetrics = serviceMetrics(
      metricsRegistry = None,
      statsReceiver = statsReceiver
    )

    assert(statsReceiver.expressions.isEmpty)
  }

  test("records successful requests") {
    basicServiceMetrics.recordStats(0, Return(0), 10)

    assert(statsReceiver.counters(Seq("requests")) == 1)
    assert(statsReceiver.counters(Seq("failures")) == 0)
    assert(statsReceiver.counters(Seq("success")) == 1)
    assert(statsReceiver.stats(Seq("request_latency_ms")) == Seq(10.0f))
  }

  test("records failed requests") {
    basicServiceMetrics.recordStats(0, Throw(new Exception("so sad")), 10)

    assert(statsReceiver.counters(Seq("requests")) == 1)
    assert(statsReceiver.counters(Seq("failures")) == 1)
    assert(statsReceiver.counters(Seq("success")) == 0)
    assert(statsReceiver.stats(Seq("request_latency_ms")) == Seq(10.0f))
  }

  test("Honors the response classifier") {
    basicServiceMetrics.recordStats(-1, Return(0), 10)
    basicServiceMetrics.recordStats(0, Return(-1), 11)

    assert(statsReceiver.counters(Seq("requests")) == 2)
    assert(statsReceiver.counters(Seq("failures")) == 2)
    assert(statsReceiver.counters(Seq("success")) == 0)
    assert(statsReceiver.stats(Seq("request_latency_ms")) == Seq(10.0f, 11.0f))
  }

  test("source failures") {
    val esh = new CategorizingExceptionStatsHandler(sourceFunction = _ => Some("bogus"))
    val basicServiceMetrics = serviceMetrics(exceptionStatsHandler = esh)

    val e = new Failure("e").withSource(Failure.Source.Service, "bogus")
    basicServiceMetrics.recordStats(0, Throw(e), 10)

    val sourced = statsReceiver.counters.filterKeys { _.exists(_ == "sourcedfailures") }
    assert(sourced.size == 2)
    assert(sourced(Seq("sourcedfailures", "bogus")) == 1)
    assert(sourced(Seq("sourcedfailures", "bogus", classOf[Failure].getName())) == 1)

    val unsourced = statsReceiver.counters.filterKeys { _.exists(_ == "failures") }
    assert(unsourced.size == 2)
    assert(unsourced(Seq("failures")) == 1)
    assert(unsourced(Seq("failures", classOf[Failure].getName())) == 1)
  }

}
