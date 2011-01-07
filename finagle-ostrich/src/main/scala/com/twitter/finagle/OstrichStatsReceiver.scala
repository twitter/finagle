package com.twitter.finagle

import com.twitter.ostrich.Stats
import com.twitter.finagle.stats.StatsReceiver

class OstrichStatsReceiver extends StatsReceiver {
  def observer(prefix: String, label: String) = {
    val suffix = "_%s".format(label)

    (path: Seq[String], value: Int, count: Int) => {
      // Enforce count == 1?
      val pathString = path mkString "__"
      Stats.addTiming(prefix + pathString, value)
      Stats.addTiming(prefix + pathString + suffix, value)
    }
  }

  def makeGauge(name: String, f: => Float) {
    Stats.makeGauge(name)(f)
  }
}