package com.twitter.finagle.stats

trait OCounter extends com.twitter.finagle.stats.Counter {
  def sum: Int
}

trait OGauge extends com.twitter.finagle.stats.Gauge {
  /**
   * An atomic snapshot of summary statistics.
   */
  case class Summary(total: Float, count: Int)

  /**
   * Arithmetic mean
   */
  def mean = {
    val snapshot = summary
    snapshot.total / snapshot.count
  }

  /**
   * Get an atomic snapshot of summary statistics
   */
  def summary: Summary
}


/**
 * A service for storing and collecting statistics. The kinds of data
 * that can be measured include Counters (which maintains only a sum)
 * and Gauges (which maintains a set of summary statistics such as
 * mean).
 */
trait StatsRepository extends StatsReceiver {

  /**
   *  Get a Counter with the description
   */
  def counter(description: (String, String)*): OCounter

  /**
   * Get a Gauge with the given description
   */
  def gauge(description: (String, String)*): OGauge

  /**
   * Prepends a prefix description to all descriptions on this StatsRepository
   */
  def scope(prefix: (String, String)*) = {
    val self = this
    new StatsRepository {
      def counter(description: (String, String)*): OCounter =
        self.counter(prefix ++ description: _*)

      def gauge(description: (String, String)*): OGauge =
        self.gauge(prefix ++ description: _*)

      def mkGauge(description: Seq[(String, String)], f: => Float) {
        self.mkGauge(prefix ++ description, f)
      }
    }
  }
}

/**
 * A StatsRepository that discards all data
 */
class NullStatsRepository extends StatsRepository {
  private[this] class Gauge extends OGauge {
    val summary = Summary(0.0f, 0)
    def measure(value: Float) {}
  }

  private[this] class Counter extends OCounter {
    def incr(delta: Int) {}
    val sum = 0
  }

  def gauge(description: (String, String)*): OGauge = new Gauge
  def counter(description: (String, String)*): OCounter = new Counter
  def mkGauge(description: Seq[(String, String)], f: => Float) {}
}