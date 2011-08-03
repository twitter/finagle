package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import org.mockito.ArgumentCaptor
import com.twitter.util.Future
import org.specs.Specification
import org.specs.mock.Mockito
import org.apache.thrift.protocol.{TMessageType, TMessage}
import com.twitter.finagle.tracing._
import com.twitter.finagle.Filter._

class ThriftClientFramedCodecSpec extends Specification with Mockito {

  "ThriftClientFramedCodec" should {

    "set sampled boolean correctly" in {
      val tracer = mock[Tracer]
      tracer.sampleTrace(any(classManifest[TraceId])) returns Some(true)

      Trace.clear()

      val filter = new ThriftClientTracingFilter("service", true)
      val buffer = new OutputBuffer()
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
      val options = new thrift.TraceOptions
      options.write(buffer())
      buffer().writeMessageEnd()

      val tracing = new TracingFilter[ThriftClientRequest, Array[Byte]](tracer)
      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      service(_request.capture) returns Future(Array[Byte]())

      val stack = tracing andThen filter
      stack(new ThriftClientRequest(buffer.toArray, false), service)

      val header = new thrift.TracedRequestHeader
      InputBuffer.peelMessage(_request.getValue.message, header)

      header.isSampled mustBe true
    }
  }
}