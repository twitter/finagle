package com.twitter.finagle.stats

/**
 * AdditiveGauges provide composite gauges on top of Ostrich. This
 * allows us to roll up gauge values.
 */

import collection.mutable.HashMap
import com.twitter.stats.Stats

object AdditiveGauges {
  private[this] val gauges = new HashMap[String, Seq[() => Float]]

  def apply(name: String)(f: => Float) = synchronized {
    val fs = Seq({ () => f })

    if (gauges contains name) {
      gauges(name) = gauges(name) ++ fs
    } else {
      gauges(name) = fs
      Stats.addGauge(name) {
        AdditiveGauges.this.synchronized { gauges(name) map (_()) sum }
      }
    }
  }
}
