package com.twitter.finagle.zipkin.core

import com.twitter.finagle.zipkin.initialSampleRate
import org.scalatest.FunSuite

class DefaultSamplerTest extends FunSuite {

  test("initialSampleRate flag overrides the default sample rate") {
    initialSampleRate.let(0.42f) {
      DefaultSampler.reset()
      assert(DefaultSampler.sampleRate == .42f)
    }
  }

  test("default sample rate can be overridden if the initialSampleRate flag is not set") {
    DefaultSampler.setSampleRate(.99f)
    assert(DefaultSampler.sampleRate == .99f)
  }

  test("default sample rate can not be overridden if the initialSampleRate flag is set") {
    initialSampleRate.let(0.63f) {
      DefaultSampler.reset()
      DefaultSampler.setSampleRate(.99f)
      assert(DefaultSampler.sampleRate == .63f)
    }

  }

}
