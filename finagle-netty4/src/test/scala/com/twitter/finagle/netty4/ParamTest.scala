package com.twitter.finagle.netty4

import com.twitter.finagle.Stack
import org.scalatest.funsuite.AnyFunSuite

class ParamTest extends AnyFunSuite {
  test("WorkerPool is global") {
    val params = Stack.Params.empty
    // make sure that we have referential equality across
    // param invocations.
    val e0 = params[param.WorkerPool].eventLoopGroup
    val e1 = params[param.WorkerPool].eventLoopGroup
    assert(e0 eq e1)
  }
}
