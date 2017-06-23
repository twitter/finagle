package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.common.metrics.{AbstractGauge, HistogramInterface, Metrics}
import com.twitter.finagle.http.{HttpMuxHandler, Route, RouteIndex}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.lint.{Category, GlobalRules, Issue, Rule}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import scala.collection.JavaConverters._

// The ordering issue is that LoadService is run early in the startup
// lifecycle, typically before Flags are loaded. By using a system
// property you can avoid that brittleness.
object debugLoggedStatNames extends GlobalFlag[Set[String]](
  Set.empty,
  "Comma separated stat names for logging observed values" +
    " (set via a -D system property to avoid load ordering issues)"
)

// It's possible to override the scope separator (the default value for `MetricsStatsReceiver` is
// `"/"`), which is used to separate scopes defined by  `StatsReceiver`. This flag might be useful
// while migrating from Commons Stats (i.e., `CommonsStatsReceiver`), which is configured to use
// `"_"` as scope separator.
object scopeSeparator extends GlobalFlag[String](
  "/",
  "Override the scope separator."
)

object MetricsStatsReceiver {
  val defaultRegistry = Metrics.root()
  private[this] val _defaultHostRegistry = Metrics.createDetached()
  val defaultHostRegistry = _defaultHostRegistry

  private def defaultFactory(name: String): HistogramInterface =
    new MetricsBucketedHistogram(name)

  /**
   * A semi-arbitrary value, but should a service call any counter/stat/addGauge
   * this often, it's a good indication that they are not following best practices.
   */
  private val CreateRequestLimit = 100000L

  private[this] case class CounterIncrData(name: String, value: Long)
  private[this] case class StatAddData(name: String, delta: Long)
}

/**
 * This implementation of StatsReceiver uses the [[com.twitter.common.metrics]] library under
 * the hood.
 *
 * Note: Histogram uses [[com.twitter.common.stats.WindowedApproxHistogram]] under the hood.
 * It is (by default) configured to store events in a 80 seconds moving window, reporting
 * metrics on the first 60 seconds. It means that when you add a value, you need to wait at most
 * 20 seconds before this value will be aggregated in the exported metrics.
 */
class MetricsStatsReceiver(
    val registry: Metrics,
    histogramFactory: String => HistogramInterface)
  extends StatsReceiverWithCumulativeGauges with WithHistogramDetails {

  import MetricsStatsReceiver._

  def this(registry: Metrics) = this(registry, MetricsStatsReceiver.defaultFactory)
  def this() = this(MetricsStatsReceiver.defaultRegistry)

  def repr: MetricsStatsReceiver = this

  // Use for backward compatibility with ostrich caching behavior
  private[this] val counters = new ConcurrentHashMap[Seq[String], Counter]
  private[this] val stats = new ConcurrentHashMap[Seq[String], Stat]

  // Used to store underlying histogram counts
  private[this] val histoDetails = new ConcurrentHashMap[String, HistogramDetail]

  private[this] val log = Logger.get()

  private[this] val loggedStats: Set[String] = debugLoggedStatNames()

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
  private[this] val separator: String = scopeSeparator()
  require(separator.length == 1, s"Scope separator should be one symbol: '$separator'")

  override def toString: String = "MetricsStatsReceiver"

  /**
   * Create and register a counter inside the underlying Metrics library
   */
  def counter(names: String*): Counter = {
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.counter on $names")
    counterRequests.increment()
    var counter = counters.get(names)
    if (counter == null) counters.synchronized {
      counter = counters.get(names)
      if (counter == null) {
        counter = new Counter {
          val metricsCounter = registry.createCounter(format(names))
          def incr(delta: Int): Unit = {
            metricsCounter.add(delta)
          }
        }
        counters.put(names, counter)
      }
    }
    counter
  }

  /**
   * Create and register a stat (histogram) inside the underlying Metrics library
   */
  def stat(names: String*): Stat = {
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.stat for $names")
    statRequests.increment()
    var stat = stats.get(names)
    if (stat == null) stats.synchronized {
      stat = stats.get(names)
      if (stat == null) {
        val doLog = loggedStats.contains(format(names))
        stat = new Stat {
          val histogram = histogramFactory(format(names))
          registry.registerHistogram(histogram)
          def add(value: Float): Unit = {
            if (doLog) log.info(s"Stat ${histogram.getName()} observed $value")
            val asLong = value.toLong
            histogram.add(asLong)
          }
          // Provide read-only access to underlying histogram through histoDetails
          val statName = format(names)
          histogram match {
            case histo: MetricsBucketedHistogram =>
              histoDetails.put(statName, histo.histogramDetail)
            case _ =>
              log.debug(s"$statName's histogram implementation doesn't support details")
          }
        }
        stats.put(names, stat)
      }
    }
    stat
  }

  override def addGauge(name: String*)(f: => Float): Gauge = {
    if (log.isLoggable(Level.TRACE))
      log.trace(s"Calling StatsReceiver.addGauge for $name")
    gaugeRequests.increment()
    super.addGauge(name: _*)(f)
  }

  protected[this] def registerGauge(names: Seq[String], f: => Float) {
    val gauge = new AbstractGauge[java.lang.Double](format(names)) {
      override def read = new java.lang.Double(f)
    }
    registry.register(gauge)
  }

  protected[this] def deregisterGauge(names: Seq[String]) {
    registry.unregister(format(names))
  }

  private[this] def format(names: Seq[String]) = names.mkString(separator)

  def histogramDetails: Map[String, HistogramDetail] = histoDetails.asScala.toMap

}

class MetricsExporter(val registry: Metrics)
  extends JsonExporter(registry)
  with HttpMuxHandler
  with MetricsRegistry
{
  def this() = this(MetricsStatsReceiver.defaultRegistry)
  val pattern = "/admin/metrics.json"
  def route: Route = Route(
    pattern = pattern,
    handler = this,
    index = Some(RouteIndex(
      alias = "Metrics",
      group = "Metrics",
      path = Some("/admin/metrics.json?pretty=true"))))
}
