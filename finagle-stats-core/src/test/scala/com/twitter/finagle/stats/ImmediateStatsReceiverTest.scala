package com.twitter.finagle.stats

import org.scalatest.FunSuite

class ImmediateStatsReceiverTest extends FunSuite {

  test("ImmediateStatsReceiver report adds immediately") {
    val histo = ImmediateMetricsHistogram("", IndexedSeq.empty)

    1.to(100).foreach { histo.add(_) }
    val snap = histo.snapshot
    assert(snap.count == 100)
    assert(snap.min == 1)
    assert(snap.max == 100)
  }
}
