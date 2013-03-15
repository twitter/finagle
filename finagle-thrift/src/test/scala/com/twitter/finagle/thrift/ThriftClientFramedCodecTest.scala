package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.tracing._
import com.twitter.util.Future
import org.apache.thrift.protocol.{TMessageType, TMessage, TBinaryProtocol}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ThriftClientFramedCodecTest extends FunSuite with MockitoSugar {
  val protocolFactory = new TBinaryProtocol.Factory()

  test("ThriftClientFramedCodec should set sampled boolean correctly") {
    val tracer = mock[Tracer]
    //tracer.sampleTrace(any(classManifest[TraceId])) returns Some(true)
    when(tracer.sampleTrace(any(classOf[TraceId]))).thenReturn(Some(true))

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
    when(service(_request.capture)).thenReturn(Future(Array[Byte]()))

    val stack = tracing andThen filter
    stack(new ThriftClientRequest(buffer.toArray, false), service)

    val header = new thrift.RequestHeader
    InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

    assert(header.isSampled)
  }

  test("ThriftClientFramedCodec should create header correctly") {
    Trace.unwind {
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
      when(service(_request.capture)).thenReturn(Future(Array[Byte]()))

      filter(new ThriftClientRequest(buffer.toArray, false), service)

      val header = new thrift.RequestHeader
      InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

      assert(header.getTrace_id === 1L)
      assert(header.getSpan_id === 2L)
      assert(! header.isSetParent_span_id)
      assert(header.isSampled)
      assert(header.isSetFlags)
      assert(header.getFlags === 1L)
    }
  }
}
