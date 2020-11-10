package com.twitter.finagle.client

import com.twitter.finagle.{ClientParamsInjector, Stack}
import com.twitter.finagle.util.TestParam

class TestClientParamsInjector extends ClientParamsInjector {
  def name: String = "test-injector"
  def apply(params: Stack.Params): Stack.Params = params + TestParam(37)
}
