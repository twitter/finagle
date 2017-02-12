package com.twitter.finagle.stats

import com.twitter.common.metrics.Snapshot
import scala.collection.Map

/**
 * Struct representing the sampled values from
 * the Metrics registry.
 */
private[stats] case class SampledValues(
    gauges: Map[String, Number],
    counters: Map[String, Number],
    histograms: Map[String, Snapshot])
