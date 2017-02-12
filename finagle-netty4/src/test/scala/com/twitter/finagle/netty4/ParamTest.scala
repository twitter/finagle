package com.twitter.finagle.netty4

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.Stack

@RunWith(classOf[JUnitRunner])
class ParamTest extends FunSuite {
  test("WorkerPool is global") {
    val params = Stack.Params.empty
    // make sure that we have referential equality across
    // param invocations.
    val e0 = params[param.WorkerPool].eventLoopGroup
    val e1 = params[param.WorkerPool].eventLoopGroup
    assert(e0 eq e1)
  }
}