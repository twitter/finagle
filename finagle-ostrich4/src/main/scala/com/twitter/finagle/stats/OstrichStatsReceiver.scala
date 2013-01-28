package com.twitter.finagle.stats

import com.twitter.ostrich.stats.{Stats, StatsCollection}

class OstrichStatsReceiver(
  val repr: StatsCollection = Stats,
  val delimiter: String = "/"
) extends StatsReceiverWithCumulativeGauges {

  // To avoid breaking the Java API:
  def this(repr: StatsCollection) = this(repr, "/")

  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    repr.addGauge(variableName(name)) { f.toDouble }
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    repr.clearGauge(variableName(name))
  }

  def counter(name: String*) = new Counter {
    private[this] val name_ = variableName(name)

    def incr(delta: Int) { repr.incr(name_, delta) }
  }

  def stat(name: String*) = new Stat {
    private[this] val name_ = variableName(name)

    def add(value: Float) {
      repr.addMetric(name_, value.toInt)
    }
  }

  private[this] def variableName(name: Seq[String]) = name mkString delimiter
}
