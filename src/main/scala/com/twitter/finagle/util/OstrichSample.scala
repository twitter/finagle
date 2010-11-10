package com.twitter.finagle.util

import com.twitter.ostrich.StatsProvider

class OstrichAddableSample(provider: StatsProvider, name: String) extends AddableSample {
  def add(value: Int, count: Int) { provider.incr(name, count) }
  def sum = provider.getCounterStats(false)(name).toInt
  def count = sum
}
