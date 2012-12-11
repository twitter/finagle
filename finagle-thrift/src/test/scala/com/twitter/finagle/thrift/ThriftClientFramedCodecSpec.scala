package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import org.mockito.ArgumentCaptor
import com.twitter.util.Future
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.apache.thrift.protocol.{TMessageType, TMessage, TBinaryProtocol}
import com.twitter.finagle.tracing._
import com.twitter.finagle.Filter._

class ThriftClientFramedCodecSpec extends SpecificationWithJUnit with Mockito {
  val protocolFactory = new TBinaryProtocol.Factory()

  "ThriftClientFramedCodec" should {

    "set sampled boolean correctly" in {
      val tracer = mock[Tracer]
      tracer.sampleTrace(any(classManifest[TraceId])) returns Some(true)

      Trace.clear()

      val filter = new ThriftClientTracingFilter("service", true, None, protocolFactory)
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
      val options = new thrift.ConnectionOptions
      options.write(buffer())
      buffer().writeMessageEnd()

      val tracing = new TracingFilter[ThriftClientRequest, Array[Byte]](tracer)
      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      service(_request.capture) returns Future(Array[Byte]())

      val stack = tracing andThen filter
      stack(new ThriftClientRequest(buffer.toArray, false), service)

      val header = new thrift.RequestHeader
      InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

      header.isSampled mustBe true
    }

    "create header correctly" in {
      val traceId = TraceId(Some(SpanId(1L)), None, SpanId(2L), Some(true), Flags().setDebug)
      Trace.setId(traceId)

      val filter = new ThriftClientTracingFilter("service", true, None, protocolFactory)
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(new TMessage("method", TMessageType.CALL, 0))

      val options = new thrift.ConnectionOptions
      options.write(buffer())
      buffer().writeMessageEnd()

      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      service(_request.capture) returns Future(Array[Byte]())

      filter(new ThriftClientRequest(buffer.toArray, false), service)

      val header = new thrift.RequestHeader
      InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

      header.getTrace_id mustBe 1L
      header.getSpan_id mustBe 2L
      header.isSetParent_span_id mustBe false
      header.isSampled mustBe true
      header.isSetFlags mustBe true
      header.getFlags mustBe 1L
    }
  }
}
