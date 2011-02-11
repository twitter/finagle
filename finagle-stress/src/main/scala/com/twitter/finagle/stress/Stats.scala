package com.twitter.finagle.stress

import java.io.PrintStream

import com.twitter.ostrich.StatsProvider
import com.twitter.ostrich

object Stats {
  private[this] def print(message: String) {
    (new PrintStream(System.out, true, "UTF-8")).println(message)
  }

  def prettyPrintStats() {
    prettyPrint(ostrich.Stats)
    prettyPrintGauges()
  }

  def prettyPrint(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      print("# %-60s %d".format(name, count))
    }

    stats.getTimingStats foreach { case (name, stat) =>
      val statMap = stat.toMap
      val keys = statMap.keys.toList.sorted

      keys foreach { key =>
        print("⊕ %-60s %s".format("%s/%s".format(name, key), statMap(key)))
      }
    }
  }

  def prettyPrintGauges() {
    ostrich.Stats.getGaugeStats(false) foreach { case (k, v) =>
      print("≈ %-60s %s".format(k, v))
    }
  }
}
