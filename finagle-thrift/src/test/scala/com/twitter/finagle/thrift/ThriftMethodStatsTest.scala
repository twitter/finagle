package com.twitter.finagle.thrift

import com.twitter.finagle.stats.InMemoryStatsReceiver
import org.scalatest.funsuite.AnyFunSuite

class ThriftMethodStatsTest extends AnyFunSuite {
  test("ThriftMethodStats counters are lazy") {
    val sr = new InMemoryStatsReceiver
    val stats = ThriftMethodStats(sr)
    assert(sr.counters.isEmpty)
  }
}
