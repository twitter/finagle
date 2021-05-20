package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.{Http, ThriftMux}
import org.scalatest.funsuite.AnyFunSuite

class StackServerOpsTest extends AnyFunSuite {

  test("Http.withOpenCensusTracing") {
    import StackServerOps._

    val server = Http.server
    assert(!server.stack.contains(ServerTraceContextFilter.role))
    assert(!server.stack.contains(StackServerOps.HttpDeserializationStackRole))

    val serverWithOC = server.withOpenCensusTracing
    assert(serverWithOC.stack.contains(ServerTraceContextFilter.role))
    assert(serverWithOC.stack.contains(StackServerOps.HttpDeserializationStackRole))
  }

  test("ThriftMux.withOpenCensusTracing") {
    import StackServerOps._

    val server = ThriftMux.server
    assert(!server.stack.contains(ServerTraceContextFilter.role))

    val serverWithOC = server.withOpenCensusTracing
    assert(serverWithOC.stack.contains(ServerTraceContextFilter.role))
  }

}
