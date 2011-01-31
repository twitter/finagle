package com.twitter.finagle.stats

trait ReadableCounter extends Counter {
  def sum: Int
}

/**
 * An atomic snapshot of summary statistics.
 */
case class Summary(total: Float, count: Int) {
  /**
   * Arithmetic mean
   */
  val mean = total / count
}

trait ReadableGauge extends Gauge {
  /**
   * Arithmetic mean
   */
  def mean = summary.mean

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
  def counter(description: (String, String)*): ReadableCounter

  /**
   * Get a Gauge with the given description
   */
  def gauge(description: (String, String)*): ReadableGauge

  /**
   * Multiplex measurements to a StatsReceiver
   */
  def reportTo(receiver: StatsReceiver) = {
    val self = this
    new StatsRepository {
      def mkGauge(description: Seq[(String, String)], f: => Float) {
        self.mkGauge(description, f)
        receiver.mkGauge(description, f)
      }

      def gauge(description: (String, String)*) = new ReadableGauge {
        private[this] val underlying = self.gauge(description: _*)

        def measure(value: Float) {
          underlying.measure(value)
          receiver.gauge(description: _*).measure(value)
        }

        def summary = underlying.summary
      }

      def counter(description: (String, String)*) = new ReadableCounter {
        private[this] val underlying = self.counter(description: _*)

        def incr(delta: Int) {
          underlying.incr(delta)
          receiver.counter(description: _*).incr(delta)
        }

        def sum = underlying.sum
      }
    }
  }
}

/**
 * A StatsRepository that discards all data
 */
class NullStatsRepository extends StatsRepository {
  def gauge(description: (String, String)*) = new ReadableGauge {
    val summary = Summary(0.0f, 0)
    def measure(value: Float) {}
  }

  def counter(description: (String, String)*) = new ReadableCounter {
    def incr(delta: Int) {}
    val sum = 0
  }

  def mkGauge(description: Seq[(String, String)], f: => Float) {}
}
