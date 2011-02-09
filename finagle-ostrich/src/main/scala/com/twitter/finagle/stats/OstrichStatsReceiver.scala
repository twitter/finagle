package com.twitter.finagle.stats

import com.twitter.ostrich.Stats

class OstrichStatsReceiver extends StatsReceiver {
  def counter(name: String*) = new Counter {
    private[this] val name_ = variableName(name)

    def incr(delta: Int) { Stats.incr(name_, delta) }
  }

  def stat(name: String*) = new Stat {
    private[this] val name_ = variableName(name)

    def add(value: Float, count: Int) {
      // TODO: can't really implement this properly... should we take
      // the average over `count' and add `count' samples?
      Stats.addTiming(name_, value.toInt)
    }
  }

  def provideGauge(name: String*)(f: => Float) = {
    AdditiveGauges(variableName(name))(f)
  }

  private[this] def variableName(name: Seq[String]) = name mkString "/"
}
