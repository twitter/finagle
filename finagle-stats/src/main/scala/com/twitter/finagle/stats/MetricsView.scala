package com.twitter.finagle.stats

import java.util

/**
 * Provides snapshots of metrics values.
 */
private[stats] trait MetricsView {

  /**
   * A snapshot of instantaneous values for all gauges.
   */
  def gauges: util.Map[String, Number]

  /**
   * A snapshot of instantaneous values for all counters.
   */
  def counters: util.Map[String, Number]

  /**
   * A snapshot of instantaneous values for all histograms.
   */
  def histograms: util.Map[String, Snapshot]

  /**
   * A snapshot of verbosity levels attached to each metric. For the sake of efficiency,
   * metrics with verbosity [[Verbosity.Default]] aren't included into a returned map.
   */
  def verbosity: util.Map[String, Verbosity]
}
