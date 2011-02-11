package com.twitter.finagle.stress

import java.io.PrintStream

import com.twitter.stats.{Stats => OstrichStats}
import com.twitter.stats.StatsProvider

object Stats {
  private[this] def print(message: String) {
    (new PrintStream(System.out, true, "UTF-8")).println(message)
  }

  def prettyPrintStats() {
    prettyPrint(OstrichStats)
    prettyPrintGauges()
  }

  def prettyPrint(stats: StatsProvider) {
    stats.getCounters foreach { case (name, count) =>
    }

    stats.getMetrics foreach { case (name, stat) =>
      val statMap = stat.toMap
      val keys = statMap.keys.toList.sorted

      keys foreach { key =>
        print("⊕ %-60s %s".format("%s/%s".format(name, key), statMap(key)))
      }
    }
  }

  def prettyPrintGauges() {
    OstrichStats.getGauges foreach { case (k, v) =>
      print("≈ %-60s %s".format(k, v))
    }
  }
}
