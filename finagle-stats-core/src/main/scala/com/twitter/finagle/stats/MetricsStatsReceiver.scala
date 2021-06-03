package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.finagle.http.{HttpMuxHandler, Route, RouteIndex}
import com.twitter.finagle.stats.MetricBuilder.{CounterType, GaugeType, HistogramType}
import com.twitter.finagle.stats.exp.{ExpressionSchema, ExpressionSchemaKey}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.{Level, Logger}
import com.twitter.util.lint.{Category, GlobalRules, Issue, Rule}
import com.twitter.util.{Future, FuturePool, Time}
import java.util.concurrent.Executor
import java.util.concurrent.atomic.LongAdder
import scala.collection.JavaConverters._

// The ordering issue is that LoadService is run early in the startup
// lifecycle, typically before Flags are loaded. By using a system
// property you can avoid that brittleness.
object debugLoggedStatNames
    extends GlobalFlag[Set[String]](
      Set.empty,
      "Comma separated stat names for logging observed values" +
        " (set via a -D system property to avoid load ordering issues)"
    )

// It's possible to override the scope separator (the default value for `MetricsStatsReceiver` is
// `"/"`), which is used to separate scopes defined by  `StatsReceiver`. This flag might be useful
// while migrating from Commons Stats (i.e., `CommonsStatsReceiver`), which is configured to use
// `"_"` as scope separator.
object scopeSeparator
    extends GlobalFlag[String](
      "/",
      "Override the scope separator."
    )

object MetricsStatsReceiver {
  val defaultRegistry = new Metrics()
  private[stats] val defaultHostRegistry = Metrics.createDetached()

  /**
   * A semi-arbitrary value, but should a service call any counter/stat/addGauge
   * this often, it's a good indication that they are not following best practices.
   */
  private val CreateRequestLimit = 100000L
}

/**
 * The standard implementation of StatsReceiver, optimized to be high precision
 * and low overhead.
 *
 * Note: Histogram is configured to store events in 60 second snapshots.  It
 * means that when you add a value, you need to wait at most 20 seconds before
 * this value will be aggregated in the exported metrics.
 */
class MetricsStatsReceiver(val registry: Metrics)
    extends StatsReceiverWithCumulativeGauges
    with CollisionTrackingStatsReceiver
    with WithHistogramDetails {

  /**
   * Overrides the executor that manages cumulative gauges to use the same
   * executor that backs FuturePool.unboundedPool.
   */
  override def executor: Executor = FuturePool.defaultExecutor

  import MetricsStatsReceiver._

  def this() = this(MetricsStatsReceiver.defaultRegistry)

  def repr: MetricsStatsReceiver = this

  def histogramDetails: Map[String, HistogramDetail] = registry.histoDetails.asScala.toMap

  // Used to store underlying histogram counts
  private[this] val log = Logger.get()

  private[this] val counterRequests = new LongAdder()
  private[this] val statRequests = new LongAdder()
  private[this] val gaugeRequests = new LongAdder()

  private[this] def checkRequestsLimit(which: String, adder: LongAdder): Option[Issue] = {
    // todo: ideally these would be computed as rates over time, but this is a
    // relatively simple proxy for bad behavior.
    val count = adder.sum()
    if (count > CreateRequestLimit)
      Some(Issue(s"StatReceiver.$which() has been called $count times"))
    else
      None
  }

  GlobalRules.get.add(
    Rule(
      Category.Performance,
      "Elevated metric creation requests",
      "For best performance, metrics should be created and stored in member variables " +
        "and not requested via `StatsReceiver.{counter,stat,addGauge}` at runtime. " +
        "Large numbers are an indication that these metrics are being requested " +
        "frequently at runtime."
    ) {
      Seq(
        checkRequestsLimit("counter", counterRequests),
        checkRequestsLimit("stat", statRequests),
        checkRequestsLimit("addGauge", gaugeRequests)
      ).flatten
    }
  )

  // Scope separator, a string value used to separate scopes defined by `StatsReceiver`.
  private[this] val separator: String = {
    metadataScopeSeparator.setSeparator(scopeSeparator())
    scopeSeparator()
  }
  require(separator.length == 1, s"Scope separator should be one symbol: '$separator'")

  override def toString: String = "MetricsStatsReceiver"

  /**
   * Create and register a counter inside the underlying Metrics library
   */
  def counter(metricBuilder: MetricBuilder): Counter = {
    validateMetricType(metricBuilder, CounterType)
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.counter on $metricBuilder.name")
    counterRequests.increment()

    val storeCounter = registry.getOrCreateCounter(metricBuilder)
    storeCounter.counter
  }

  /**
   * Create and register a stat (histogram) inside the underlying Metrics library
   */
  def stat(metricBuilder: MetricBuilder): Stat = {
    validateMetricType(metricBuilder, HistogramType)
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.stat for $metricBuilder.name")
    statRequests.increment()
    val storeStat = registry.getOrCreateStat(metricBuilder)
    storeStat.stat
  }

  override def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge = {
    validateMetricType(metricBuilder, GaugeType)
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.addGauge for $metricBuilder.name")
    gaugeRequests.increment()
    super.addGauge(metricBuilder)(f)
  }

  protected[this] def registerGauge(metricBuilder: MetricBuilder, f: => Float): Unit = {
    registry.registerGauge(metricBuilder, f)
  }

  protected[this] def deregisterGauge(name: Seq[String]): Unit = {
    registry.unregisterGauge(name)
  }

  override def metricsCollisionsLinterRule: Rule = registry.metricsCollisionsLinterRule

  override protected[finagle] def registerExpression(expressionSchema: ExpressionSchema): Unit = {
    registry.registerExpression(expressionSchema)
  }
}

private object MetricsExporter {
  val defaultLogger = Logger.get()
}

class MetricsExporter(val registry: Metrics, val logger: Logger)
    extends JsonExporter(registry)
    with HttpMuxHandler
    with MetricsRegistry
    with SchemaRegistry {

  def this(registry: Metrics) = this(registry, MetricsExporter.defaultLogger)

  def this(logger: Logger) = this(MetricsStatsReceiver.defaultRegistry, logger)

  def this() = this(MetricsExporter.defaultLogger)

  /**
   * Indicates use of latched or unlatched counters for SchemaRegistry.
   */
  def hasLatchedCounters: Boolean = useCounterDeltas()

  /**
   * Exposes MetricSchemas for SchemaRegistry.
   * @return a map of metric names to their full MetricSchemas.
   */
  override def schemas(): Map[String, MetricBuilder] = registry.schemas.asScala.toMap

  /**
   * Exposes Metric Expressions for ExpressionRegistry.
   * @return a map of expression names to their full ExpressionSchema.
   */
  def expressions(): Map[ExpressionSchemaKey, ExpressionSchema] =
    registry.expressions.asScala.toMap

  val pattern = "/admin/metrics.json"

  def route: Route =
    Route(
      pattern = pattern,
      handler = this,
      index = Some(
        RouteIndex(
          alias = "Metrics",
          group = "Metrics",
          path = Some("/admin/metrics.json?pretty=true")
        )
      )
    )

  override def close(deadline: Time): Future[Unit] = {
    val f: Future[Unit] = logOnShutdown() match {
      case true =>
        //Use a FuturePool to ensure the task is completed asynchronously and allow for enforcing
        //the deadline Time.
        FuturePool
          .unboundedPool {
            logger.error(json(false, false))
          }.by(deadline)(DefaultTimer)
      case _ => Future.Done
    }
    f.flatMap(_ => super.close(deadline)) //ensure parent's close is honored
  }

}
