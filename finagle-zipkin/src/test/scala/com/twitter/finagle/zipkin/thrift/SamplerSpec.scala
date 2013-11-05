package com.twitter.finagle.zipkin.thrift

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.tracing._
import com.twitter.util.Time

class SamplerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
  val traceIdSampled = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))

  "Sampler" should {
    "throw exception if illegal sample rate" in {
      val sampler = new Sampler
      sampler.setSampleRate(-1) must throwA[IllegalArgumentException]
      sampler.setSampleRate(1.1f) must throwA[IllegalArgumentException]
    }

    "check if valid sample rate" in {
      val sampler = new Sampler
      sampler.validSampleRate(-1) mustEqual false
      sampler.validSampleRate(1.1f) mustEqual false
      sampler.validSampleRate(0.5f) mustEqual true
    }

    "drop all by setting samplerate" in {
      val sampler = new Sampler
      sampler.setSampleRate(0)
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(false)
      }
    }

    "drop none by setting samplerate" in {
      val sampler = new Sampler
      sampler.setSampleRate(1f)
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(true)
      }
    }

    "drop all by providing samplerate" in {
      val sampler = new Sampler
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 0f) mustEqual Some(false)
      }
    }

    "drop none by providing samplerate" in {
      val sampler = new Sampler
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None), 1f) mustEqual Some(true)
      }
    }

    "sample record if sample is true" in {
      val sampler = new Sampler
      sampler.setSampleRate(0f)
      sampler.sampleRecord(Record(traceIdSampled, Time.now, Annotation.ClientSend())) mustEqual true
    }

    "sample record if sample is none and sample rate 1" in {
      val sampler = new Sampler
      sampler.setSampleRate(1f)
      sampler.sampleRecord(Record(traceId, Time.now, Annotation.ClientSend())) mustEqual true
    }

  }
}
