package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import com.twitter.common.stats.{StatImpl => JStat}
import com.twitter.common.base.Supplier

class CommonsStatsReceiver extends StatsReceiverWithCumulativeGauges {
  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    Stats.STATS_PROVIDER.makeGauge(variableName(name), new Supplier[java.lang.Float]{
      def get = {f.asInstanceOf[java.lang.Float]}
    })
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    // not implmented in commons
  }

  def counter(name: String*) = new Counter {
    private[this] val name_ = variableName(name)

    def incr(delta: Int) { Stats.STATS_PROVIDER.makeCounter(name_).getAndAdd(delta) }
  }

  def stat(name: String*) = new Stat {
    private[this] val name_ = variableName(name)
    private[this] val float = 0.0f

    private[this] val jstat = new JStat[Float](name_){
      def read: Float = float
    }

    def add(value: Float) {
    }
  }

  private[this] def variableName(name: Seq[String]) = name mkString "_"
}
