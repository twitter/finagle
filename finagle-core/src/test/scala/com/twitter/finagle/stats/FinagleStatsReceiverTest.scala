package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class FinagleStatsReceiverTest extends AnyFunSuite {

  private val testImpl = new FinagleStatsReceiverImpl(new InMemoryStatsReceiver)
  private val bizBazCounter = testImpl.scope("biz").counter("baz")
  private val bizBazMetricBuilder = bizBazCounter.metadata.toMetricBuilder.get
  private val bizBazIdentity = bizBazMetricBuilder.identity

  test("Metrics get the right hierarchical scopes") {
    assert(bizBazIdentity.hierarchicalName == Seq("finagle", "biz", "baz"))
  }

  test("Metrics get the right dimensional scopes and labels") {
    assert(bizBazIdentity.dimensionalName == Seq("rpc", "finagle", "biz", "baz"))
    assert(bizBazIdentity.labels == Map("implementation" -> "finagle", "rpc_system" -> "finagle"))
  }

  test("Metrics are valid in dimensional and hierarchical form") {
    assert(!bizBazIdentity.hierarchicalOnly)
  }
}
