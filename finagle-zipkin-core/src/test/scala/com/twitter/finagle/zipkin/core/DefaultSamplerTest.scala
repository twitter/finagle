package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing.SpanId
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.zipkin.initialSampleRate
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DefaultSamplerTest extends AnyFunSuite with MockitoSugar {

  test("initialSampleRate flag overrides the default sample rate") {
    initialSampleRate.let(0.42f) {
      assert(DefaultSampler.sampleRate == .42f)
    }
  }

  test("sample rate can be overridden if the initialSampleRate flag is not set") {
    val oldSamplingRate = DefaultSampler.sampleRate
    DefaultSampler.setSampleRate(.99f)
    assert(DefaultSampler.sampleRate == .99f)
    // Resetting the value to avoid potential side effects for other tests as this is static
    DefaultSampler.setSampleRate(oldSamplingRate)
  }

  test("sample rate can not be overridden if the initialSampleRate flag is set") {
    initialSampleRate.let(0.63f) {
      DefaultSampler.setSampleRate(.99f)
      assert(DefaultSampler.sampleRate == .63f)
    }
  }

  test("sample rate cannot be > 1.0 or < 0.0") {
    intercept[IllegalArgumentException] {
      DefaultSampler.setSampleRate(100.0f)
    }
    intercept[IllegalArgumentException] {
      DefaultSampler.setSampleRate(-100.0f)
    }
  }

  test("sampling tracer with sample rate 1.0 should sample trace") {
    val underlying = mock[Tracer]
    val samplingTracer = new SamplingTracer(underlying, DefaultSampler)
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), None)
    val oldSampleRate = DefaultSampler.sampleRate

    initialSampleRate.let(1.0f) {
      assert(samplingTracer.getSampleRate == 1.0f)
      assert(samplingTracer.sampleTrace(traceId).contains(true))
    }

    assert(DefaultSampler.sampleRate != 1.0f)
    assert(DefaultSampler.sampleRate == Sampler.DefaultSampleRateInternal)
    DefaultSampler.setSampleRate(1.0f)
    assert(samplingTracer.getSampleRate == 1.0f)
    assert(samplingTracer.sampleTrace(traceId).contains(true))
    // Resetting the value to avoid potential side effects for other tests as this is static
    DefaultSampler.setSampleRate(oldSampleRate)
  }

  test("sampling tracer with sample rate 0.0 should not sample trace") {
    val underlying = mock[Tracer]
    val samplingTracer = new SamplingTracer(underlying, DefaultSampler)
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(456)), SpanId(789), None)
    val oldSampleRate = DefaultSampler.sampleRate

    initialSampleRate.let(0.0f) {
      assert(samplingTracer.getSampleRate == 0.0f)
      assert(DefaultSampler.sampleRate == 0.0f)
      assert(samplingTracer.sampleTrace(traceId).contains(false))
    }

    assert(DefaultSampler.sampleRate != 0.0f)
    assert(DefaultSampler.sampleRate == Sampler.DefaultSampleRateInternal)
    DefaultSampler.setSampleRate(0.0f)
    assert(samplingTracer.getSampleRate == 0.0f)
    assert(samplingTracer.sampleTrace(traceId).contains(false))
    // Resetting the value to avoid potential side effects for other tests as this is static
    DefaultSampler.setSampleRate(oldSampleRate)
  }

}
