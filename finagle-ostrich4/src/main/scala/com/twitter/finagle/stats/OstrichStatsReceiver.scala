package com.twitter.finagle.stats

import com.twitter.ostrich.stats.{Stats => FStats, StatsCollection}

class OstrichStatsReceiver(
    val repr: StatsCollection = FStats,
    val delimiter: String = "/")
  extends StatsReceiverWithCumulativeGauges {

  def this() = this(FStats, "/")
  // To avoid breaking the Java API:
  def this(repr: StatsCollection) = this(repr, "/")

  override def toString: String = "OstrichStatsReceiver"

  override protected[this] def registerGauge(name: Seq[String], f: => Float) {
    repr.addGauge(variableName(name)) { f.toDouble }
  }

  override protected[this] def deregisterGauge(name: Seq[String]) {
    repr.clearGauge(variableName(name))
  }

  override def counter(name: String*): Counter = new Counter {
    private[this] val counter = repr.getCounter(variableName(name))

    override def incr(delta: Int): Unit = counter.incr(delta)
  }

  override def stat(name: String*): Stat = new Stat {
    private[this] val metric = repr.getMetric(variableName(name))

    override def add(value: Float): Unit = {
      metric.add(value.toInt)
    }
  }

  private[this] def variableName(name: Seq[String]) = name.mkString(delimiter)
}
