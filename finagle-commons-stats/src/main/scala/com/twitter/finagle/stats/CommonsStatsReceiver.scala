package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import com.twitter.common.stats.Percentile
import com.twitter.common.base.Supplier
import com.twitter.common.util.Sampler

class CommonsStatsReceiver extends StatsReceiverWithCumulativeGauges {

  val statMap = collection.mutable.Map.empty[String, Stat]

  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    Stats.STATS_PROVIDER.makeGauge(variableName(name), new Supplier[java.lang.Float] {
      def get = new java.lang.Float(f)
    })
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    // not implmented in commons
  }

  def counter(name: String*) = new Counter {
    private[this] val counter = Stats.exportLong(variableName(name))
    def incr(delta: Int) { counter.addAndGet(delta) }
  }

  def stat(name: String*) = {
    statMap.getOrElseUpdate(variableName(name), new Stat {
      val percentile = new Percentile[java.lang.Float](variableName(name), 100.0f , 50, 95, 99)

      def add(value: Float) = {
        percentile.record(value)
      }
    })
  }

  private[this] def variableName(name: Seq[String]) = name mkString "_"
}
