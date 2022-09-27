package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.IdentityType
import org.scalatest.funsuite.AnyFunSuite

class RootFinagleStatsReceiverTest extends AnyFunSuite {

  private val root = new InMemoryStatsReceiver
  private val sr = new RootFinagleStatsReceiver(root, "clnt", Seq("rpc", "finagle", "client"))

  test("ClientStatsReceiver adds root scope to its .toString") {
    assert(sr.toString == "InMemoryStatsReceiver/clnt")
  }

  test("ClientStatsReceiver decorates the identity appropriately") {
    val counter = sr.counter("foo")
    val identity = counter.metadata.toMetricBuilder.get.identity

    assert(identity.identityType == IdentityType.Full)
    assert(identity.dimensionalName == Seq("rpc", "finagle", "client", "foo"))
    assert(identity.hierarchicalName == Seq("clnt", "foo"))
    assert(
      identity.labels == Map(
        "rpc_system" -> "finagle",
        "implementation" -> "finagle",
        "rpc_service" -> "clnt"))
  }

  test("empty service labels") {
    val sr = new RootFinagleStatsReceiver(root, serviceLabel = "", Seq("rpc", "finagle", "client"))

    val counter = sr.counter("foo")
    val identity = counter.metadata.toMetricBuilder.get.identity

    assert(identity.identityType == IdentityType.Full)
    assert(identity.dimensionalName == Seq("rpc", "finagle", "client", "foo"))
    assert(identity.hierarchicalName == Seq("foo"))
    assert(
      identity.labels == Map(
        "rpc_system" -> "finagle",
        "implementation" -> "finagle",
        "rpc_service" -> ""))
  }

  test("doesn't override metrics flagged hierarchical-only") {
    val sr = new RootFinagleStatsReceiver(root, serviceLabel = "", Seq("rpc", "finagle", "client"))

    val counter = sr.counter(
      MetricBuilder.forCounter.withIdentity(
        MetricBuilder.Identity(
          hierarchicalName = Seq("foo"),
          dimensionalName = Seq("foo"),
          identityType = IdentityType.HierarchicalOnly
        )
      ))

    val identity = counter.metadata.toMetricBuilder.get.identity

    assert(identity.identityType == IdentityType.HierarchicalOnly)
    assert(identity.dimensionalName == Seq("rpc", "finagle", "client", "foo"))
    assert(identity.hierarchicalName == Seq("foo"))
    assert(
      identity.labels == Map(
        "rpc_system" -> "finagle",
        "implementation" -> "finagle",
        "rpc_service" -> ""))
  }
}
