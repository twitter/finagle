package com.twitter.finagle.zipkin.thrift

import com.twitter.app.Flags
import com.twitter.finagle.zipkin.core.DefaultSampler
import com.twitter.finagle.zipkin.initialSampleRate
import org.scalatest.funsuite.AnyFunSuite

class ZipkinTracerTest extends AnyFunSuite {

  test("sampling uses the default configurable sampling rate if not overridden") {
    val tracer = new ScribeZipkinTracer()
    assert(tracer.getSampleRate == DefaultSampler.sampleRate)

    initialSampleRate.reset()
  }

  test("sampling uses the flag-configured sampling rate") {
    initialSampleRate.let(0.14f) {
      val tracer = ZipkinTracer.mk().asInstanceOf[ZipkinTracer]
      assert(tracer.getSampleRate == 0.14f)
    }
    initialSampleRate.reset()
  }

  test("sampling uses the DefaultSampler-configured sampling rate") {
    initialSampleRate.letClear {
      val prevSampleRate = DefaultSampler.sampleRate
      DefaultSampler.setSampleRate(0.28f)
      try {
        val tracer = ZipkinTracer.mk().asInstanceOf[ZipkinTracer]
        assert(tracer.getSampleRate == 0.28f)
      } finally {
        DefaultSampler.setSampleRate(prevSampleRate)
      }
    }
    initialSampleRate.reset()
  }

  test("passing sample rate in the ZipkinTracer constructor always takes precedence") {
    val flag = new Flags("foo", true)
    flag.parseArgs(Array("-com.twitter.finagle.zipkin.initialSampleRate", "0.63"))

    val tracer = new ZipkinTracer(ScribeRawZipkinTracer(), 1.0f)
    assert(tracer.getSampleRate == 1.0f)

    initialSampleRate.reset()
  }

}
