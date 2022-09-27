package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.IdentityType
import org.scalatest.funsuite.AnyFunSuite

class FinagleStatsReceiverTest extends AnyFunSuite {

  private val testImpl = new FinagleStatsReceiverImpl(new InMemoryStatsReceiver)
  private val scopedSr = testImpl.scope("biz")

  private val bizBazCounter = scopedSr.counter("baz")
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
    assert(bizBazIdentity.identityType == IdentityType.Full)
  }

  test("doesn't override metrics flagged hierarchical-only") {

    val counter = scopedSr.counter(
      MetricBuilder.forCounter.withIdentity(
        MetricBuilder.Identity(
          hierarchicalName = Seq("baz"),
          dimensionalName = Seq("baz"),
          identityType = IdentityType.HierarchicalOnly
        )
      ))

    val identity = counter.metadata.toMetricBuilder.get.identity

    assert(identity.identityType == IdentityType.HierarchicalOnly)

    assert(identity.hierarchicalName == Seq("finagle", "biz", "baz"))
    assert(identity.dimensionalName == Seq("rpc", "finagle", "biz", "baz"))
    assert(identity.labels == Map("implementation" -> "finagle", "rpc_system" -> "finagle"))
  }
}
