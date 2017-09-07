package com.twitter.finagle.stats

private[stats] object MetricsStore {

  /**
   * The internal representation of counters in a MetricsStore.
   */
  trait StoreCounter {
    def name: String

    def count: Long

    def counter: Counter
  }

  /**
   * The internal representation of gauges in a MetricsStore.
   */
  trait StoreGauge {
    def name: String

    def read: Number
  }

  /**
   * The internal representation of histograms in a MetricsStore.
   */
  trait StoreStat {
    def name: String

    def stat: Stat

    def snapshot: Snapshot

    def clear(): Unit
  }
}

/**
 * A metrics store for managing metrics.
 */
private[stats] trait MetricsStore {

  /**
   * Creates a new counter, or gets an existing one of that name if it already
   * exists.
   *
   * Deduplicates by formatted name, so that Seq("foo", "bar") and
   * Seq("foo/bar") with the separator "/" are the same name.
   *
   * Throws a [MetricCollisionException] if there's already a gauge of that name.
   */
  def getOrCreateCounter(verbosity: Verbosity, names: Seq[String]): MetricsStore.StoreCounter

  /**
   * Registers a new gauge, replacing the previous one if it already existed.
   *
   * Deduplicates by formatted name, so that Seq("foo", "bar") and
   * Seq("foo/bar") with the separator "/" are the same name.
   *
   * Throws a [MetricCollisionException] if there's already a counter of that name.
   */
  def registerGauge(verbosity: Verbosity, names: Seq[String], f: => Float): Unit

  /**
   * Deregisters a gauge.
   *
   * Deduplicates by formatted name, so that Seq("foo", "bar") and
   * Seq("foo/bar") with the separator "/" are the same name.
   */
  def unregisterGauge(names: Seq[String]): Unit

  /**
   * Registers a new stat, or gets an existing one of that name if it already
   * exists.
   *
   * Deduplicates by formatted name, so that Seq("foo", "bar") and
   * Seq("foo/bar") with the separator "/" are the same name.
   */
  def getOrCreateStat(verbosity: Verbosity, names: Seq[String]): MetricsStore.StoreStat

  /**
   * Registers a new stat that exports the given percentiles, or gets an
   * existing one of that name if it already exists.
   *
   * If a stat already exists, there is no guarantee that `percentiles` will be
   * respected.
   *
   * Deduplicates by formatted name, so that Seq("foo", "bar") and
   * Seq("foo/bar") with the separator "/" are the same name.
   */
  def getOrCreateStat(
    verbosity: Verbosity,
    names: Seq[String],
    percentiles: IndexedSeq[Double]
  ): MetricsStore.StoreStat
}
