package com.twitter.finagle.stress

import com.twitter.stats.StatsProvider

object Stats {
  def prettyPrint(stats: StatsProvider) {
    stats.getCounters foreach { case (name, count) =>
      println("# %-30s %d".format(name, count))
    }

    stats.getMetrics foreach { case (name, stat) =>
      val statMap = stat.toMap
      val keys = statMap.keys.toList.sorted

      keys foreach { key =>
        println("# %-30s %s".format("timing_%s".format(key), statMap(key)))
      }
    }
  }
}
