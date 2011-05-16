package com.twitter.finagle.stats

import com.twitter.ostrich.stats.Stats

class OstrichStatsReceiver extends StatsReceiverWithCumulativeGauges {
  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    Stats.addGauge(variableName(name)) { f.toDouble }
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    Stats.clearGauge(variableName(name))
  }

  def counter(name: String*) = new Counter {
    private[this] val name_ = variableName(name)

    def incr(delta: Int) { Stats.incr(name_, delta) }
  }

  def stat(name: String*) = new Stat {
    private[this] val name_ = variableName(name)

    def add(value: Float) {
      Stats.addMetric(name_, value.toInt)
    }
  }

  private[this] def variableName(name: Seq[String]) = name mkString "/"
}
