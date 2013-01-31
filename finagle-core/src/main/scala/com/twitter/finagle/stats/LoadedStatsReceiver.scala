package com.twitter.finagle.stats

import com.twitter.finagle.util.LoadService

/**
 * A [[com.twitter.finagle.stats.StatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
private[finagle] class LoadedStatsReceiver extends StatsReceiver with Proxy {
  lazy val self = {
    val receivers = LoadService[StatsReceiver]()
    BroadcastStatsReceiver(receivers)
  }

  lazy val repr = self

  def counter(names: String*) = self.counter(names:_*)
  def stat(names: String*) = self.stat(names:_*)
  def addGauge(names: String*)(f: => Float) = self.addGauge(names:_*)(f)
  override def scope(name: String) = self.scope(name)
}
