package com.twitter.finagle.stats

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

    assert(identity.hierarchicalOnly == false)
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

    assert(identity.hierarchicalOnly == false)
    assert(identity.dimensionalName == Seq("rpc", "finagle", "client", "foo"))
    assert(identity.hierarchicalName == Seq("foo"))
    assert(
      identity.labels == Map(
        "rpc_system" -> "finagle",
        "implementation" -> "finagle",
        "rpc_service" -> ""))
  }
}
