package com.twitter.finagle.zipkin.core

import org.scalatestplus.mockito.MockitoSugar
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.SpanId
import com.twitter.finagle.tracing.TraceId
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class SamplerTest extends AnyFunSuite with MockitoSugar {

  class SamplerHelper {
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
    val traceIdSampled = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
  }

  test("Sampler should throw exception if illegal sample rate") {
    val sampler = new Sampler
    intercept[IllegalArgumentException] {
      sampler.setSampleRate(-1)
    }
    intercept[IllegalArgumentException] {
      sampler.setSampleRate(1.1f)
    }
  }

  test("Sampler should check if valid sample rate") {
    val sampler = new Sampler
    assert(!sampler.validSampleRate(-1))
    assert(!sampler.validSampleRate(1.1f))
    assert(sampler.validSampleRate(0.5f))
  }

  test("Sampler should drop all by setting samplerate") {
    val sampler = new Sampler()
    sampler.setSampleRate(0f)
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None)).contains(false))
    }
  }

  test("Sampler should drop none by setting samplerate") {
    val sampler = new Sampler()
    sampler.setSampleRate(1f)
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None)).contains(true))
    }
  }

  test("Sampler should drop all by providing samplerate") {
    val sampler = new Sampler()
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 0f).contains(false))
    }
  }

  test("Sampler should drop none by providing samplerate") {
    val sampler = new Sampler()
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 1f).contains(true))
    }
  }

  test("Sampler should sample record if sample is true") {
    val h = new SamplerHelper
    import h._

    val sampler = new Sampler()
    sampler.setSampleRate(0f)
    assert(sampler.sampleRecord(Record(traceIdSampled, Time.now, Annotation.ClientSend)))
  }

  test("Sampler should sample record if sample is none and sample rate 1") {
    val h = new SamplerHelper
    import h._

    val sampler = new Sampler()
    sampler.setSampleRate(1f)
    assert(sampler.sampleRecord(Record(traceId, Time.now, Annotation.ClientSend)))
  }

  test("Sampler should sample at its minimum precise rate") {
    val sampler = new Sampler(1f / (1 << 24))
    var sampled = 0
    for (i <- 0 until (1 << 24)) {
      if (sampler.sampleTrace(TraceId(None, None, SpanId(i), None)).contains(true)) {
        sampled = sampled + 1
      }
    }
    assert(sampled == 1)
  }
}
