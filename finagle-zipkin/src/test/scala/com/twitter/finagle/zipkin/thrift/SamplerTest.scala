package com.twitter.finagle.zipkin.thrift

import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSuite
import com.twitter.finagle.tracing.{Annotation, Record, SpanId, TraceId}
import com.twitter.util.Time
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class SamplerTest extends FunSuite with MockitoSugar {

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
    val sampler = new Sampler
    sampler.setSampleRate(0)
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) == Some(false))
    }
  }

  test("Sampler should drop none by setting samplerate") {
    val sampler = new Sampler
    sampler.setSampleRate(1f)
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) == Some(true))
    }
  }

  test("Sampler should drop all by providing samplerate") {
    val sampler = new Sampler
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 0f) == Some(false))
    }
  }

  test("Sampler should drop none by providing samplerate") {
    val sampler = new Sampler
    for (i <- 1 until 100) {
      assert(sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 1f) == Some(true))
    }
  }

  test("Sampler should sample record if sample is true") {
    val h = new SamplerHelper
    import h._

    val sampler = new Sampler
    sampler.setSampleRate(0f)
    assert(sampler.sampleRecord(Record(traceIdSampled, Time.now, Annotation.ClientSend())))
  }

  test("Sampler should sample record if sample is none and sample rate 1") {
    val h = new SamplerHelper
    import h._

    val sampler = new Sampler
    sampler.setSampleRate(1f)
    assert(sampler.sampleRecord(Record(traceId, Time.now, Annotation.ClientSend())))
  }
}
