package com.twitter.finagle.stats

import scala.collection.Map

/**
 * Struct representing the sampled values from
 * the Metrics registry.
 */
private[twitter] case class SampledValues(
  gauges: Map[String, Number],
  counters: Map[String, Number],
  histograms: Map[String, Snapshot])
