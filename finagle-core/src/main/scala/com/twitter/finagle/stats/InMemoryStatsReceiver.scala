package com.twitter.finagle.stats

import scala.collection.mutable

/** In-memory stats receiver for testing. */
class InMemoryStatsReceiver extends StatsReceiver {
  val repr = this

  val counters = new mutable.HashMap[Seq[String], Int]
                   with mutable.SynchronizedMap[Seq[String], Int]
  val stats    = new mutable.HashMap[Seq[String], Seq[Float]]
                   with mutable.SynchronizedMap[Seq[String], Seq[Float]]
  val gauges   = new mutable.WeakHashMap[Seq[String], () => Float]
                   with mutable.SynchronizedMap[Seq[String], () => Float]

  def counter(name: String*): Counter = {
    new Counter {
      def incr(delta: Int) {
        val oldValue = counters.get(name).getOrElse(0)
        counters(name) = oldValue + delta
      }
    }
  }

  def stat(name: String*): Stat = {
    new Stat {
      def add(value: Float) {
        val oldValue = stats.get(name).getOrElse(Seq.empty)
        stats(name) = oldValue :+ value
      }
    }
  }

  def addGauge(name: String*)(f: => Float): Gauge = {
    val gauge = new Gauge {
      def remove() {
        gauges -= name
      }
    }
    gauges += name -> (() => f)
    gauge
  }
}
