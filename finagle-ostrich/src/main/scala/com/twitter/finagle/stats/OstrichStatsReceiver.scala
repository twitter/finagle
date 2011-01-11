package com.twitter.finagle.stats

import com.twitter.ostrich.Stats

class OstrichStatsReceiver extends StatsReceiver {
  def gauge(description: (String, String)*) = new Gauge {
    private[this] val name = descriptionToName(description)

    def measure(value: Float) {
      Stats.setGauge(name, value)
    }
  }

  def counter(description: (String, String)*) = new Counter {
    private[this] val name = descriptionToName(description)

    def incr(delta: Int) {
      Stats.incr(name, delta)
    }
  }

  def mkGauge(description: Seq[(String, String)], f: => Float) {
    Stats.makeGauge(descriptionToName(description))(f)
  }

  private[this] def descriptionToName(description: Seq[(String, String)]) = {
    description.map { case (key, value) =>
      "%s_%s".format(key, value)
    }.mkString("__")
  }
}