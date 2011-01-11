package com.twitter.finagle.stats

/**
 * A service for storing and collecting statistics. The kinds of data
 * that can be measured include Counters (which maintains only a sum)
 * and Gauges (which maintains a set of summary statistics such as
 * mean).
 */
trait StatsRepository extends StatsReceiver {
  trait Counter extends super.Counter {
    def sum: Int
  }

  trait Gauge extends super.Gauge {
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
   *  Get a Counter with the description
   */
  def counter(description: (String, String)*): Counter

  /**
   * Get a Gauge with the given description
   */
  def gauge(description: (String, String)*): Gauge

  /**
   * Prepends a prefix description to all descriptions on this StatsRepository
   */
  def scope(prefix: (String, String)*) = {
    val self = this
    new StatsRepository {
      def counter(description: (String, String)*) = new super.Counter {
        private[this] val underlying = self.counter(prefix ++ description: _*)

        def incr(delta: Int) { underlying.incr(delta) }
        def sum = underlying.sum
      }

      def gauge(description: (String, String)*) = new super.Gauge {
        private[this] val underlying = self.gauge(prefix ++ description: _*)

        def measure(value: Float) = underlying.measure(value)
        def summary = {
          val snapshot = underlying.summary
          Summary(snapshot.total, snapshot.count)
        }
      }

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
  def gauge(description: (String, String)*) = new super.Gauge {
    val summary = Summary(0.0f, 0)
    def measure(value: Float) {}
  }

  def counter(description: (String, String)*) = new super.Counter {
    def incr(delta: Int) {}
    val sum = 0
  }

  def mkGauge(description: Seq[(String, String)], f: => Float) {}
}