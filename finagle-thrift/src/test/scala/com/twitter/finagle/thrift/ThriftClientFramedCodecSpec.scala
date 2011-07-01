package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import org.mockito.ArgumentCaptor
import com.twitter.util.Future
import org.specs.Specification
import org.specs.mock.Mockito
import org.apache.thrift.protocol.{TMessageType, TMessage}
import com.twitter.finagle.tracing.{TraceId, Tracer, Trace}

class ThriftClientFramedCodecSpec extends Specification with Mockito {

  "ThriftClientFramedCodec" should {

    "not set trace id if trace is sampled" in {
      val tracer = mock[Tracer]
      tracer.sampleTrace(any(classManifest[TraceId])) returns true

      Trace.clear()
      Trace.pushTracer(tracer)
      Trace.pushId()

      val filter = new ThriftClientTracingFilter("service", true)
      val buffer = new OutputBuffer()
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
      val options = new thrift.TraceOptions
      options.write(buffer())
      buffer().writeMessageEnd()

      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])

      service(_request.capture) returns Future(Array[Byte]())

      filter((new ThriftClientRequest(buffer.toArray, false)), service)
      val header = new thrift.TracedRequestHeader
      InputBuffer.peelMessage(_request.getValue.message, header)

      header.isSetTrace_id mustBe false
    }
  }
}