package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import com.twitter.common.stats.{StatImpl => JStat}
import com.twitter.common.base.Supplier

class CommonsStatsReceiver extends StatsReceiverWithCumulativeGauges {
  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    Stats.STATS_PROVIDER.makeGauge(variableName(name), new Supplier[java.lang.Float]{
      def get = {new java.lang.Float(f)}
    })
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    // not implmented in commons
  }

  def counter(name: String*) = new Counter {
    private[this] val name_ = variableName(name)
    private[this] val counter = Stats.exportLong(name_)
    def incr(delta: Int) { counter.addAndGet(delta) }
  }

  def stat(name: String*) = new Stat {
    private[this] val name_ = variableName(name)
    @volatile
    private[this] var float = 0.0f

    private[this] val jstat = new JStat[Float](name_){
      def read: Float = float
    }

    Stats.exportStatic(jstat)

    def add(value: Float) {
      float += value
    }
  }

  private[this] def variableName(name: Seq[String]) = name mkString "_"
}
