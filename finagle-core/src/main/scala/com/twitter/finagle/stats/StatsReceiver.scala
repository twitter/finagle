package com.twitter.finagle.stats

trait StatsReceiver {
  def observer(prefix: String, label: String): (Seq[String], Int, Int) => Unit
  def makeGauge(name: String, f: => Float)
}
