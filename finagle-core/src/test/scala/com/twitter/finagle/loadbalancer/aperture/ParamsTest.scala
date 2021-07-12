package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections
import org.scalatest.funsuite.AnyFunSuite

class ParamsTest extends AnyFunSuite {

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

  test("EagerConnections respects the flag value without explicit param") {
    val eagerConnections = EagerConnections.param.getDefault
    assert(eagerConnections.enabled == true)

    // if the param is explicitly configured the flag is ignored, always
    apertureEagerConnections.let(EagerConnectionsType.Disable) {
      val eagerConnectionsEnabled = EagerConnections()
      assert(eagerConnectionsEnabled.enabled == true)
    }

    apertureEagerConnections.let(EagerConnectionsType.Enable) {
      val eagerConnectionsEnabled = EagerConnections()
      assert(eagerConnectionsEnabled.enabled == true)
    }

    apertureEagerConnections.let(EagerConnectionsType.Disable) {
      val eagerConnectionsExplicitTrue = EagerConnections(true)
      assert(eagerConnectionsExplicitTrue.enabled == true)
    }

    apertureEagerConnections.let(EagerConnectionsType.Enable) {
      val eagerConnectionsExplicitFalse = EagerConnections(false)
      assert(eagerConnectionsExplicitFalse.enabled == false)
    }

    // because the EagerConnections.param is a val, we cannot use GlobalFlag scoping
    // after EagerConnections class loading and value assignment has occurred, so
    // the scope will always be ignored. given that users can configure the stack param,
    // it's likely not worth changing the vals to defs. if that changes, these tests would
    // then honor the flag value

    apertureEagerConnections.let(EagerConnectionsType.Disable) {
      val eagerConnectionsDisabled = EagerConnections.param.getDefault
      assert(eagerConnectionsDisabled.enabled == true) // ignored
    }

    apertureEagerConnections.let(EagerConnectionsType.Enable) {
      val eagerConnectionsEnabled = EagerConnections.param.getDefault
      assert(eagerConnectionsEnabled.enabled == true) // flag is ignored, this is from initial val
    }

  }
}
