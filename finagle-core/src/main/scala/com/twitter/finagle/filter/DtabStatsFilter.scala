package com.twitter.finagle.filter

import com.twitter.finagle.{Dtab, Service, SimpleFilter}
import com.twitter.finagle.stats.StatsReceiver

/**
 * Adds a Stat, dtab/local/size, that tracks the size of Dtab.local for all
 * requests with a non-empty Dtab.
 */
class DtabStatsFilter[Req, Rsp](statsReceiver: StatsReceiver)
    extends SimpleFilter[Req, Rsp] {

  private[this] val dtabSizes = statsReceiver.stat("dtab", "size")

  def apply(request: Req, service: Service[Req, Rsp]) = {
    if (Dtab.local.nonEmpty) {
      dtabSizes.add(Dtab.local.size)
    }
    service(request)
  }
}
