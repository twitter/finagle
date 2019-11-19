package com.twitter.finagle.thrift

import com.twitter.finagle.stats.InMemoryStatsReceiver
import org.scalatest.FunSuite

class ThriftMethodStatsTest extends FunSuite {
  test("ThriftMethodStats counters are lazy") {
    val sr = new InMemoryStatsReceiver
    val stats = ThriftMethodStats(sr)
    assert(sr.counters.isEmpty)
  }
}
