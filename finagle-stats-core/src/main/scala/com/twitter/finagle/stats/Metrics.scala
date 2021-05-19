package com.twitter.finagle.stats

import com.twitter.finagle.stats.exp.Expression.replaceExpression
import com.twitter.finagle.stats.exp._
import com.twitter.logging.Logger
import com.twitter.util.lint.{Category, Issue, Rule}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object Metrics {

  private val log = Logger.get()

  private def defaultHistogramFactory(
    name: String,
    percentiles: IndexedSeq[Double]
  ): MetricsHistogram = new MetricsBucketedHistogram(name, percentiles)

  // represents a real instance of a gauge or a counter
  private sealed trait Repr
  private case object GaugeRepr extends Repr
  private case object CounterRepr extends Repr

  private val DefaultMetricsMaps: MetricsMaps = newMetricsMaps

  /**
   * Create a new [[Metrics]] that does not share gauges, counters, or stats (by default,
   * these are shared across [[Metrics]] instances)
   */
  def createDetached(
    mkHistogram: (String, IndexedSeq[Double]) => MetricsHistogram,
    separator: String
  ): Metrics =
    new Metrics(mkHistogram, separator, newMetricsMaps)

  def createDetached(): Metrics =
    createDetached(Metrics.defaultHistogramFactory, scopeSeparator())

  private[this] def newMetricsMaps: MetricsMaps = MetricsMaps(
    countersMap = new ConcurrentHashMap[Seq[String], MetricsStore.StoreCounter](),
    statsMap = new ConcurrentHashMap[Seq[String], MetricsStore.StoreStat](),
    gaugesMap = new ConcurrentHashMap[Seq[String], MetricsStore.StoreGauge](),
    /** Store MetricSchemas for each metric in order to surface metric metadata to users. */
    metricSchemas = new ConcurrentHashMap[String, MetricSchema](),
    expressionSchemas = new ConcurrentHashMap[ExpressionSchemaKey, ExpressionSchema](),
    // Memoizing metrics used for building expressions.
    // the key is the object reference hashcode shared between Expression and StatsReceiver,
    // and the value is the fully hydrated metric builder in StatsReceiver.
    // Use the value to replace metric builders carried in Expression which do not have full
    // information.
    metricBuilders = new ConcurrentHashMap[Int, MetricBuilder]()
  )

  private class StoreCounterImpl(override val name: String, _metadata: Metadata)
      extends MetricsStore.StoreCounter {
    private[this] val adder = new LongAdder()

    val counter: Counter = new Counter {
      def incr(delta: Long): Unit = {
        adder.add(delta)
      }

      def metadata: Metadata = _metadata
    }

    def count: Long = adder.sum()
  }

  private class StoreGaugeImpl(override val name: String, f: => Number)
      extends MetricsStore.StoreGauge {
    override def read: Number = f
  }

  private class StoreStatImpl(
    histo: MetricsHistogram,
    override val name: String,
    doLog: Boolean,
    _metadata: Metadata)
      extends MetricsStore.StoreStat {
    def snapshot: Snapshot = histo.snapshot

    override val stat: Stat = new Stat {
      def add(value: Float): Unit = {
        if (doLog)
          log.info(s"Stat $name observed $value")
        val asLong = value.toLong
        histo.add(asLong)
      }

      def metadata: Metadata = _metadata
    }

    def clear(): Unit = histo.clear()
  }

  private case class MetricsMaps(
    countersMap: ConcurrentHashMap[Seq[String], MetricsStore.StoreCounter],
    statsMap: ConcurrentHashMap[Seq[String], MetricsStore.StoreStat],
    gaugesMap: ConcurrentHashMap[Seq[String], MetricsStore.StoreGauge],
    metricSchemas: ConcurrentHashMap[String, MetricSchema],
    expressionSchemas: ConcurrentHashMap[ExpressionSchemaKey, ExpressionSchema],
    metricBuilders: ConcurrentHashMap[Int, MetricBuilder])
}

/**
 * Thrown when you try to create a metric which would collide with a pre-existing metric.
 */
private[stats] class MetricCollisionException(msg: String) extends IllegalArgumentException(msg)

/**
 * A concrete metrics registry for creating and reading metrics.
 *
 * This stats implementation respects [[Verbosity verbosity levels]] such that
 *
 *  - it takes [[Verbosity]] as an argument while creating a certain metric and
 *  - reports verbosity levels via [[MetricsView#verbosity]]
 *
 * @note For efficiency reasons, it doesn't keep track of default (i.e., [[Verbosity.Default]])
 *       metrics.
 *
 * @note A verbosity level is only attached once, when metric is being created. Any subsequent
 *       creation/querying of the same metric (i.e., metric with the same name), doesn't affect
 *       its initial verbosity.
 *
 * @note By default, instances of [[Metrics]] share underlying [[Metrics.MetricsMaps]]. In the
 *       case of multiple [[StatsReceiver]]s, this avoids duplicate metrics. To use per-instance
 *       [[Metrics.MetricsMaps]], create the instance using `Metrics.createDetached`.
 */
private[finagle] class Metrics private (
  mkHistogram: (String, IndexedSeq[Double]) => MetricsHistogram,
  separator: String,
  metricsMaps: Metrics.MetricsMaps)
    extends MetricsStore
    with MetricsView {

  def this() = this(Metrics.defaultHistogramFactory, scopeSeparator(), Metrics.DefaultMetricsMaps)

  def this(mkHistogram: (String, IndexedSeq[Double]) => MetricsHistogram, separator: String) =
    this(mkHistogram, separator, Metrics.DefaultMetricsMaps)

  import Metrics._

  private[this] val loggedStats: Set[String] = debugLoggedStatNames()

  private[this] val countersMap = metricsMaps.countersMap

  private[this] val statsMap = metricsMaps.statsMap

  private[this] val gaugesMap = metricsMaps.gaugesMap

  private[this] val metricSchemas = metricsMaps.metricSchemas

  private[this] val expressionSchemas = metricsMaps.expressionSchemas

  private[this] val metricBuilders = metricsMaps.metricBuilders

  private[this] val verbosityMap =
    new ConcurrentHashMap[String, Verbosity]()

  private[this] val reservedNames = new ConcurrentHashMap[String, Repr]()

  val histoDetails = new ConcurrentHashMap[String, HistogramDetail]

  private[this] def format(names: Seq[String]): String =
    names.mkString(separator)

  def getOrCreateCounter(schema: CounterSchema): MetricsStore.StoreCounter = {
    val counter = countersMap.get(schema.metricBuilder.name)
    if (counter != null)
      return counter

    val formatted = format(schema.metricBuilder.name)
    val curNameUsage = reservedNames.putIfAbsent(formatted, CounterRepr)

    if (curNameUsage == null || curNameUsage == CounterRepr) {
      val next = new Metrics.StoreCounterImpl(formatted, schema.metricBuilder)
      val prev = countersMap.putIfAbsent(schema.metricBuilder.name, next)

      if (schema.metricBuilder.verbosity != Verbosity.Default)
        verbosityMap.put(formatted, schema.metricBuilder.verbosity)

      if (prev != null) {
        prev
      } else {
        metricSchemas.put(formatted, schema)
        storeMetricBuilder(schema)
        next
      }
    } else {
      throw new MetricCollisionException(
        s"A gauge with the name $formatted had already" +
          " been defined when you tried to add a new counter."
      )
    }
  }

  def getOrCreateStat(schema: HistogramSchema): MetricsStore.StoreStat = {
    val stat = statsMap.get(schema.metricBuilder.name)
    if (stat != null)
      return stat

    if (schema.metricBuilder.percentiles.isEmpty) {
      createStat(
        HistogramSchema(
          schema.metricBuilder.withPercentiles(BucketedHistogram.DefaultQuantiles: _*)))
    } else {
      createStat(schema)
    }
  }

  private def createStat(schema: HistogramSchema): MetricsStore.StoreStat = {
    val formatted = format(schema.metricBuilder.name)
    val doLog = loggedStats.contains(formatted)
    val histogram = mkHistogram(formatted, schema.metricBuilder.percentiles)

    histogram match {
      case histo: MetricsBucketedHistogram =>
        histoDetails.put(formatted, histo.histogramDetail)
      case _ =>
        log.debug(s"$formatted's histogram implementation doesn't support details")
    }

    val next = new Metrics.StoreStatImpl(histogram, formatted, doLog, schema.metricBuilder)
    val prev = statsMap.putIfAbsent(schema.metricBuilder.name, next)

    if (schema.metricBuilder.verbosity != Verbosity.Default) {
      verbosityMap.put(formatted, schema.metricBuilder.verbosity)
    }

    if (prev != null) {
      prev
    } else {
      metricSchemas.put(formatted, schema)
      storeMetricBuilder(schema)
      next
    }
  }

  private[this] def storeMetricBuilder(schema: MetricSchema): Unit = {
    schema.metricBuilder.kernel match {
      case Some(kernel) => metricBuilders.put(kernel, schema.metricBuilder)
      case None =>
    }
  }

  private[this] def removeMetricBuilder(formattedName: String): Unit = {
    val metricSchema = metricSchemas.get(formattedName)
    if (metricSchema != null) {
      metricSchema.metricBuilder.kernel match {
        case Some(kernel) => metricBuilders.remove(kernel)
        case None =>
      }
    }
  }

  private[stats] def registerExpression(exprSchema: ExpressionSchema): Unit = {
    expressionSchemas.putIfAbsent(exprSchema.schemaKey(), exprSchema)
  }

  def registerGauge(schema: GaugeSchema, f: => Float): Unit =
    registerNumberGauge(schema, f)

  def registerLongGauge(schema: GaugeSchema, f: => Long): Unit =
    registerNumberGauge(schema, f)

  private def registerNumberGauge(schema: GaugeSchema, f: => Number): Unit = {
    val formatted = format(schema.metricBuilder.name)
    val curNameUsage = reservedNames.putIfAbsent(formatted, GaugeRepr)

    if (curNameUsage == null) {
      val next = new Metrics.StoreGaugeImpl(formatted, f)
      gaugesMap.putIfAbsent(schema.metricBuilder.name, next)
      metricSchemas.putIfAbsent(formatted, schema)
      storeMetricBuilder(schema)

      if (schema.metricBuilder.verbosity != Verbosity.Default) {
        verbosityMap.put(formatted, schema.metricBuilder.verbosity)
      }
    } else if (curNameUsage == GaugeRepr) {
      // it should be impossible to collide with a gauge in finagle since
      // StatsReceiverWithCumulativeGauges already protects us.
      // we replace existing gauges to support commons metrics behavior.
      val next = new Metrics.StoreGaugeImpl(formatted, f)
      gaugesMap.put(schema.metricBuilder.name, next)
      metricSchemas.put(formatted, schema)
      storeMetricBuilder(schema)
    } else {
      throw new MetricCollisionException(
        s"A Counter with the name $formatted had already" +
          " been defined when you tried to add a new gauge."
      )
    }
  }

  def unregisterGauge(names: Seq[String]): Unit = {
    gaugesMap.remove(names)
    val formatted = format(names)
    // remove from metricBuilders must prior to remove from metricSchemas
    removeMetricBuilder(formatted)
    metricSchemas.remove(formatted)
    reservedNames.remove(formatted)
    verbosityMap.remove(formatted)
  }

  def gauges: util.Map[String, Number] = {
    val map = new util.HashMap[String, Number](gaugesMap.size)
    gaugesMap.elements().asScala.foreach { sg =>
      try {
        map.put(sg.name, sg.read)
      } catch {
        case NonFatal(e) => log.warning(e, s"exception while sampling gauge '${sg.name}'")
      }
    }
    util.Collections.unmodifiableMap(map)
  }

  def counters: util.Map[String, Number] = {
    val cs = countersMap
      .elements()
      .asScala
      .map { sc => sc.name -> Long.box(sc.count) }
      .toMap
    util.Collections.unmodifiableMap(cs.asJava)
  }

  def histograms: util.Map[String, Snapshot] = {
    val snaps = statsMap
      .elements()
      .asScala
      .map { ss => ss.name -> ss.snapshot }
      .toMap
    util.Collections.unmodifiableMap(snaps.asJava)
  }

  def verbosity: util.Map[String, Verbosity] =
    util.Collections.unmodifiableMap(verbosityMap)

  def schemas: util.Map[String, MetricSchema] =
    util.Collections.unmodifiableMap(metricSchemas)

  private[this] val filledExpression: ConcurrentHashMap[
    ExpressionSchemaKey,
    ExpressionSchema
  ] =
    new ConcurrentHashMap[ExpressionSchemaKey, ExpressionSchema]()

  def expressions: util.Map[ExpressionSchemaKey, ExpressionSchema] = {
    val added = expressionSchemas.keySet().asScala &~ filledExpression.keySet().asScala

    added.foreach { key =>
      val exprSchema = expressionSchemas.get(key)
      val newExpression = exprSchema.copy(expr = replaceExpression(exprSchema.expr, metricBuilders))
      filledExpression.putIfAbsent(key, newExpression)
    }

    util.Collections.unmodifiableMap(filledExpression)
  }

  def metricsCollisionsLinterRule: Rule =
    Rule(
      Category.Configuration,
      "Metrics name collision",
      "Identifies metrics with ambiguous names that collide with other metrics. " +
        """Metrics recorded in a scope Seq("foo", "bar") can collide with Seq("foo/bar") when """ +
        s"exporting the metrics to JSON. To fix, never use the separator character $separator " +
        "in metrics names.\nThis linter does not account for denylisted metrics, verbosity, " +
        "or collisions between Stats and Counters/Gauges."
    ) {
      def toIssue(kind: String, collisions: Iterable[Seq[String]]) =
        Issue(
          collisions
            .map(_.mkString("Seq(\"", "\", \"", "\")"))
            .mkString(s"$kind:\n", " collides with\n", "")
        )

      def toMapWithIssues[V <: MetricsStore.StoreMetric](
        map: ConcurrentHashMap[Seq[String], V],
        namesToIssue: Iterable[Seq[String]] => Issue
      ) =
        map.asScala
          .groupBy { case (_, metric) => metric.name }
          .values
          .filter(_.size > 1)
          .map(collisions => namesToIssue(collisions.keys))

      toMapWithIssues(gaugesMap, toIssue("Gauge", _)).toSeq ++
        toMapWithIssues(countersMap, toIssue("Counter", _)) ++
        toMapWithIssues(statsMap, toIssue("Stat", _))
    }
}
