package com.twitter.finagle.zipkin.thrift

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.tracing._

object SamplerSpec extends Specification with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

  "Sampler" should {
    "throw exception if illegal sample rate" in {
      val sampler = new Sampler {}
      sampler.setSampleRate(-1) must throwA[IllegalArgumentException]
      sampler.setSampleRate(1.1f) must throwA[IllegalArgumentException]
    }

    "drop all" in {
      val sampler = new Sampler {}
      sampler.setSampleRate(0)
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(false)
      }
    }

    "drop none" in {
      val sampler = new Sampler {}
      sampler.setSampleRate(1f)
      for (i <- 1 until 100) {
        sampler.sampleTrace(TraceId(None, None, SpanId(i), None)) mustEqual Some(true)
      }
    }
  }
}
