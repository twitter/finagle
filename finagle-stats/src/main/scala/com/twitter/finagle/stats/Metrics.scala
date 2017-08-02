package com.twitter.finagle.stats

import com.twitter.logging.Logger
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[stats] object Metrics {

  val log = Logger.get()

  private def defaultHistogramFactory(
    name: String,
    percentiles: IndexedSeq[Double]
  ): MetricsHistogram = new MetricsBucketedHistogram(name, percentiles)

  // represents a real instance of a gauge or a counter
  private sealed trait Repr
  private case object GaugeRepr extends Repr
  private case object CounterRepr extends Repr

  class StoreCounterImpl(override val name: String) extends MetricsStore.StoreCounter {
    private[this] val adder = new LongAdder()

    val counter: Counter = new Counter {
      def incr(delta: Long): Unit = {
        adder.add(delta)
      }
    }

    def count: Long = adder.sum()
  }

  class StoreGaugeImpl(override val name: String, f: => Float) extends MetricsStore.StoreGauge {

    def read: Float =
      f
  }

  class StoreStatImpl(histo: MetricsHistogram, override val name: String, doLog: Boolean)
      extends MetricsStore.StoreStat {
    def snapshot: Snapshot = histo.snapshot

    override val stat: Stat = new Stat {
      def add(value: Float): Unit = {
        if (doLog)
          log.info(s"Stat $name observed $value")
        val asLong = value.toLong
        histo.add(asLong)
      }
    }

    def clear(): Unit = histo.clear()
  }

}

/**
 * Thrown when you try to create a metric which would collide with a pre-existing metric.
 */
private[stats] class MetricCollisionException(msg: String) extends IllegalArgumentException(msg)

/**
 * A concrete metrics store for creating and reading metrics.
 */
private[finagle] class Metrics(
  mkHistogram: (String, IndexedSeq[Double]) => MetricsHistogram,
  separator: String
) extends MetricsStore
    with MetricsView {

  def this() = this(Metrics.defaultHistogramFactory, scopeSeparator())

  import Metrics._

  private[this] val loggedStats: Set[String] = debugLoggedStatNames()

  private[this] val countersMap =
    new ConcurrentHashMap[Seq[String], MetricsStore.StoreCounter]()

  private[this] val statsMap =
    new ConcurrentHashMap[Seq[String], MetricsStore.StoreStat]()

  private[this] val gaugesMap =
    new ConcurrentHashMap[Seq[String], MetricsStore.StoreGauge]()

  private[this] val reservedNames = new ConcurrentHashMap[String, Repr]()

  val histoDetails = new ConcurrentHashMap[String, HistogramDetail]

  private[this] def format(names: Seq[String]): String =
    names.mkString(separator)

  def getOrCreateCounter(names: Seq[String]): MetricsStore.StoreCounter = {
    val counter = countersMap.get(names)
    if (counter != null)
      return counter

    val formatted = format(names)
    val curNameUsage = reservedNames.putIfAbsent(formatted, CounterRepr)

    if (curNameUsage == null || curNameUsage == CounterRepr) {
      val next = new Metrics.StoreCounterImpl(formatted)
      val prev = countersMap.putIfAbsent(names, next)
      if (prev != null) prev else next
    } else {
      throw new MetricCollisionException(
        s"A gauge with the name $formatted had already" +
          " been defined when you tried to add a new counter."
      )
    }
  }

  def getOrCreateStat(names: Seq[String]): MetricsStore.StoreStat =
    getOrCreateStat(names, BucketedHistogram.DefaultQuantiles)

  def getOrCreateStat(
    names: Seq[String],
    percentiles: IndexedSeq[Double]
  ): MetricsStore.StoreStat = {
    val stat = statsMap.get(names)
    if (stat != null)
      return stat

    val name = format(names)
    val doLog = loggedStats.contains(name)
    val histogram = mkHistogram(name, percentiles)

    histogram match {
      case histo: MetricsBucketedHistogram =>
        histoDetails.put(name, histo.histogramDetail)
      case _ =>
        log.debug(s"$name's histogram implementation doesn't support details")
    }

    val next = new Metrics.StoreStatImpl(histogram, name, doLog)
    val prev = statsMap.putIfAbsent(names, next)
    if (prev != null) prev else next
  }

  def registerGauge(names: Seq[String], f: => Float): Unit = {
    val formatted = format(names)
    val curNameUsage = reservedNames.putIfAbsent(formatted, GaugeRepr)

    if (curNameUsage == null) {
      val next = new Metrics.StoreGaugeImpl(formatted, f)
      gaugesMap.putIfAbsent(names, next)
    } else if (curNameUsage == GaugeRepr) {
      // it should be impossible to collide with a gauge in finagle since
      // StatsReceiverWithCumulativeGauges already protects us.
      // we replace existing gauges to support commons metrics behavior.
      val next = new Metrics.StoreGaugeImpl(formatted, f)
      gaugesMap.put(names, next)
    } else {
      throw new MetricCollisionException(
        s"A Counter with the name $formatted had already" +
          " been defined when you tried to add a new gauge."
      )
    }
  }

  def unregisterGauge(names: Seq[String]): Unit = {
    gaugesMap.remove(names)
  }

  def gauges: util.Map[String, Number] = {
    val map = new util.HashMap[String, Number]()
    val gs = gaugesMap.asScala.foreach {
      case (names, sg) =>
        val key = format(names)
        try {
          map.put(key, Float.box(sg.read))
        } catch {
          case NonFatal(e) => log.warning(e, s"exception while sampling gauge '$key'")
        }
    }
    util.Collections.unmodifiableMap(map)
  }

  def counters: util.Map[String, Number] = {
    val cs = countersMap.asScala.map {
      case (names, sc) =>
        format(names) -> Long.box(sc.count)
    }
    util.Collections.unmodifiableMap(cs.asJava)
  }

  def histograms: util.Map[String, Snapshot] = {
    val snaps = statsMap.asScala.map {
      case (names, ss) =>
        format(names) -> ss.snapshot
    }
    util.Collections.unmodifiableMap(snaps.asJava)
  }
}
