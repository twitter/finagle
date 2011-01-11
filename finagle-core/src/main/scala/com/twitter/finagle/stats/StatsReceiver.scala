package com.twitter.finagle.stats

/**
 * A readable and writeable Counter. Only sums are kept of Counters.
 * An example Counter is "number of requests served".
 */
trait Counter extends {
  def incr(delta: Int)
  def incr() { incr(1) }
}

/**
 * A readable and writeable Gauge. Gauages are usually continuous
 * values that are measured at moments in time (e.g., the value
 * of a share of Twitter's stock).
 */
trait Gauge {
  /**
   * Record a measurement
   */
  def measure(value: Float)
}

trait StatsReceiver {
  /**
   *  Get a Counter with the description
   */
  def counter(description: (String, String)*): Counter

  /**
   * Get a Gauge with the description
   */
  def gauge(description: (String, String)*): Gauge

  /**
   * Register a function to be periodically measured.
   */
  def mkGauge(description: Seq[(String, String)], f: => Float)

  /**
   * Convenvenience function to deal with 1-arity descriptions
   */
  def mkGauge(description: (String, String), f: => Float) {
    mkGauge(Seq(description), f)
  }

  /**
   * Convenvenience function to deal with 2-arity descriptions
   */
  def mkGauge(description1: (String, String), description2: (String, String), f: => Float) {
    mkGauge(Seq(description1, description2), f)
  }
}
