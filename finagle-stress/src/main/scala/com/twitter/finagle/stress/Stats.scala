package com.twitter.finagle.stress

import com.twitter.ostrich.StatsProvider

object Stats {
  def prettyPrint(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      println("# %-30s %d".format(name, count))
    }

    stats.getTimingStats foreach { case (name, stat) =>
      val statMap = stat.toMap
      val keys = statMap.keys.toList.sorted

      keys foreach { key =>
        println("# %-30s %s".format("timing_%s".format(key), statMap(key)))
      }
    }
  }
}
