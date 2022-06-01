package com.twitter.finagle.server

import com.twitter.finagle.ServerParamsInjector
import com.twitter.finagle.Stack
import com.twitter.finagle.util.TestParam

class TestServerParamsInjector extends ServerParamsInjector {
  def name: String = "test-injector"
  def apply(params: Stack.Params): Stack.Params = params + TestParam(38)
}
