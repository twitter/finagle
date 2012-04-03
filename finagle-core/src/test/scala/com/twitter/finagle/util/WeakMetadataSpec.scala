package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.finagle.Service

class WeakMetadataSpec extends SpecificationWithJUnit with Mockito {
  "WeakMetadata" should {
    val s0 = mock[Service[Any, Any]]
    val s1 = mock[Service[Any, Any]]

    class IntContainer(initialValue: Int) { var value = initialValue }

    "provide default values for unknown services" in {
      var invocations = 0
      val meta = WeakMetadata[IntContainer] { invocations += 1; new IntContainer(0) }
      meta(s0).value must be_==(0)
      invocations must be_==(1)
      meta(s1).value must be_==(0)
      invocations must be_==(2)
    }

    "maintain updated values" in {
      val meta = WeakMetadata[IntContainer] { new IntContainer(0) }
      meta(s0).value = 123
      meta(s0).value must be_==(123)
      meta(s1).value must be_==(0)
    }
  }
}

  

