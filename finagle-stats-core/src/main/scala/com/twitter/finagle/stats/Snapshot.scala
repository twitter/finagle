package com.twitter.finagle.stats

object Snapshot {
  case class Percentile(quantile: Double, value: Long)
}

/**
 * A snapshot of the state of the underlying stat.
 */
trait Snapshot {

  /**
   * The number of times the stat was reported.
   */
  def count: Long

  /**
   * The overall sum of the reported values.
   */
  def sum: Long

  /**
   * The largest reported values
   */
  def max: Long

  /**
   * The smallest reported value.
   */
  def min: Long

  /**
   * The average reported value.
   */
  def average: Double

  /**
   * The values reported for given quantiles.
   */
  def percentiles: IndexedSeq[Snapshot.Percentile]

}
