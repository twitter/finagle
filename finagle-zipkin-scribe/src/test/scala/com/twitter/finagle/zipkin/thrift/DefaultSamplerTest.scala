package com.twitter.finagle.zipkin.thrift

import com.twitter.app.Flags
import com.twitter.finagle.zipkin.core.Sampler
import com.twitter.finagle.zipkin.initialSampleRate
import org.scalatest.FunSuite

class DefaultSamplerTest extends FunSuite {

  test("sampling uses the static default sampling rate if not overridden") {
    val tracer = new ScribeZipkinTracer()
    assert(tracer.getSampleRate == Sampler.DefaultSampleRate)
  }

  test("initialSampleRate flag overrides the default sample rate") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.42"))
    DefaultSampler.reset()

    val tracer = new ScribeZipkinTracer()
    assert(tracer.getSampleRate == .42f)

    initialSampleRate.reset()
  }

  test("default sample rate can be overridden if the initialSampleRate flag is not set") {
    val tracer = new ScribeZipkinTracer()
    DefaultSampler.setSampleRate(.99f)
    assert(tracer.getSampleRate == .99f)
  }

  test("default sample rate can not be overridden if the initialSampleRate flag is set") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.63"))
    DefaultSampler.reset()

    val tracer = new ScribeZipkinTracer()
    DefaultSampler.setSampleRate(.99f)
    assert(tracer.getSampleRate == .63f)

    initialSampleRate.reset()
  }

  test("passing sample rate in the ZipkinTracer constructor always takes precedence") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.63"))
    DefaultSampler.reset()

    val tracer = new ZipkinTracer(ScribeRawZipkinTracer(), 1.0f)
    assert(tracer.getSampleRate == 1.0f)
  }
}
