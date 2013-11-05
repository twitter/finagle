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

  def counter(name: String*): ReadableCounter = 
    new ReadableCounter {
      def incr(delta: Int) {
        val oldValue = apply
        counters(name) = oldValue + delta
      }
      def apply(): Int = counters.get(name).getOrElse(0)
    }

  def stat(name: String*): ReadableStat =
    new ReadableStat {
      def add(value: Float) {
        val oldValue = apply
        stats(name) = oldValue :+ value
      }
      def apply(): Seq[Float] = stats.get(name).getOrElse(Seq.empty)
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

trait ReadableCounter extends Counter {
  def apply(): Int
}

trait ReadableStat extends Stat {
  def apply(): Seq[Float]
}
