package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections
import org.scalatest.FunSuite

class ParamsTest extends FunSuite {

  test("EagerConnections takes on the default flag value unless set") {
    // ensure that there's a default value
    assert(apertureEagerConnections.getWithDefault.isDefined)
    val default = apertureEagerConnections.getWithDefault.get

    val params: Stack.Params = Stack.Params.empty
    assert(params[EagerConnections].enabled == default)

    // purposefully make it the opposite
    val setParams = params + EagerConnections(!default)
    assert(setParams[EagerConnections].enabled == !default)

    apertureEagerConnections.reset()
  }

  test("EagerConnections can change its value") {
    var enabled = false

    val eagerConnections = EagerConnections(() => enabled)
    assert(eagerConnections.enabled == false)

    enabled = true
    assert(eagerConnections.enabled == true)
  }
}
