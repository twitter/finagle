package com.twitter.finagle.stats

import com.twitter.util.MapMaker
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.concurrent.Serialized

class SimpleStatsRepository extends StatsRepository {
  private[this] class Counter extends ReadableCounter {
    private[this] val _sum = new AtomicInteger(0)

    def incr(delta: Int) { _sum.addAndGet(delta) }
    def sum = _sum.get
  }

  private[this] class Gauge extends ReadableGauge with Serialized {
    @volatile private[this] var _summary = Summary(0.0f, 0)

    def measure(value: Float) {
      serialized {
        val snapshot = summary
        _summary = Summary(snapshot.total + value, snapshot.count + 1)
      }
    }

    def summary = _summary
  }

  private[this] val counters = MapMaker[Seq[(String, String)], Counter] { config =>
    config.compute { _ => new Counter }
  }

  private[this] val gauges   = MapMaker[Seq[(String, String)], Gauge] { config =>
    config.compute { _ => new Gauge }
  }

  def counter(path: (String, String)*): ReadableCounter = counters(path)
  def gauge(path: (String, String)*): ReadableGauge = gauges(path)

  /**
   * Unsupported for now.
   */
  def mkGauge(path: Seq[(String, String)], f: => Float) {}
}