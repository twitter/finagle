package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.{Http, ThriftMux}
import org.scalatest.funsuite.AnyFunSuite

class StackClientOpsTest extends AnyFunSuite {

  test("Http.withOpenCensusTracing") {
    import StackClientOps._

    val client = Http.client
    assert(!client.stack.contains(ClientTraceContextFilter.role))
    assert(!client.stack.contains(StackClientOps.HttpSerializationStackRole))

    val clientWithOC = client.withOpenCensusTracing
    assert(clientWithOC.stack.contains(ClientTraceContextFilter.role))
    assert(clientWithOC.stack.contains(StackClientOps.HttpSerializationStackRole))
  }

  test("ThriftMux.withOpenCensusTracing") {
    import StackClientOps._

    val client = ThriftMux.client
    assert(!client.stack.contains(ClientTraceContextFilter.role))

    val clientWithOC = client.withOpenCensusTracing
    assert(clientWithOC.stack.contains(ClientTraceContextFilter.role))
  }

}
