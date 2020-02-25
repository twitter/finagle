package com.twitter.finagle.zipkin.core

import com.twitter.app.Flags
import com.twitter.finagle.zipkin.initialSampleRate
import org.scalatest.FunSuite

class DefaultSamplerTest extends FunSuite {

  test("initialSampleRate flag overrides the default sample rate") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.42"))
    DefaultSampler.reset()

    assert(DefaultSampler.sampleRate == .42f)

    initialSampleRate.reset()
  }

  test("default sample rate can be overridden if the initialSampleRate flag is not set") {
    DefaultSampler.setSampleRate(.99f)
    assert(DefaultSampler.sampleRate == .99f)
  }

  test("default sample rate can not be overridden if the initialSampleRate flag is set") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.63"))
    DefaultSampler.reset()

    DefaultSampler.setSampleRate(.99f)
    assert(DefaultSampler.sampleRate == .63f)

    initialSampleRate.reset()
  }

}
