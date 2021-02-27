package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections
import org.scalatest.FunSuite

class ParamsTest extends FunSuite {

  test("EagerConnections takes on the default flag value unless set") {
    // ensure that there's a default value
    assert(apertureEagerConnections.getWithDefault.isDefined)
    val default: EagerConnectionsType.Value = apertureEagerConnections.getWithDefault.get
    val defaultBool = if (default == EagerConnectionsType.Disable) false else true

    val params: Stack.Params = Stack.Params.empty
    assert(params[EagerConnections].enabled == defaultBool)

    // purposefully make it the opposite
    val setParams = params + EagerConnections(!defaultBool)
    assert(setParams[EagerConnections].enabled == !defaultBool)

    apertureEagerConnections.reset()
  }

}
