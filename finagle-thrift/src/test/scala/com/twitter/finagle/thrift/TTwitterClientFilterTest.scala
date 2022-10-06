package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.tracing.ClientTracingFilter.TracingFilter
import com.twitter.finagle.tracing._
import com.twitter.io.Buf
import com.twitter.util.Future
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TBinaryProtocol
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class TTwitterClientFilterTest extends AnyFunSuite with MockitoSugar {
  val protocolFactory = new TBinaryProtocol.Factory()

  test("TTwitterClientFilter should set sampled boolean correctly") {
    val tracer = mock[Tracer]
    //tracer.sampleTrace(any(classManifest[TraceId])) returns Some(true)
    when(tracer.sampleTrace(any(classOf[TraceId]))).thenReturn(Some(true))

    val filter = new TTwitterClientFilter("service", true, None, protocolFactory)
    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
    val options = new thrift.ConnectionOptions
    options.write(buffer())
    buffer().writeMessageEnd()

    val tracing = new TraceInitializerFilter[ThriftClientRequest, Array[Byte]](tracer, true)
      .andThen(new TracingFilter[ThriftClientRequest, Array[Byte]]("TTwitterClientFilterTest"))
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
    when(service(_request.capture)).thenReturn(Future(Array[Byte]()))

    val stack = tracing andThen filter
    stack(new ThriftClientRequest(buffer.toArray, false), service)

    val header = new thrift.RequestHeader
    InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

    assert(header.isSampled)
  }

  test("TTwitterClientFilter should create header correctly") {
    val traceId = TraceId(Some(SpanId(1L)), None, SpanId(2L), Some(true), Flags().setDebug)
    Trace.letId(traceId) {

      val filter = new TTwitterClientFilter("service", true, None, protocolFactory)
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

      assert(header.getTrace_id == 1L)
      assert(header.getSpan_id == 2L)
      assert(!header.isSetParent_span_id)
      assert(header.isSampled)
      assert(header.isSetFlags)
      assert(header.getFlags == 1L)
    }
  }

  test("TTwitterClientFilter should set ClientId in both header and context") {
    val clientId = ClientId("foo.bar")
    val filter = new TTwitterClientFilter("service", true, Some(clientId), protocolFactory)
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

    assert(header.getContexts != null)
    val clientIdContextWasSet = header.getContexts.asScala exists { c =>
      (Buf.ByteArray.Owned(c.getKey()) == ClientId.clientIdCtx.marshalId) &&
      (Buf.ByteArray.Owned(c.getValue()) == Buf.Utf8(clientId.name))
    }

    assert(header.getClient_id.getName == clientId.name)
    assert(clientIdContextWasSet == true)
  }

  test("TTwitterClientFilter should not be overrideable with externally-set ClientIds") {
    val clientId = ClientId("foo.bar")
    val otherClientId = ClientId("other.bar")
    val filter = new TTwitterClientFilter("service", true, Some(clientId), protocolFactory)
    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(new TMessage("method", TMessageType.CALL, 0))

    val options = new thrift.ConnectionOptions
    options.write(buffer())
    buffer().writeMessageEnd()

    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val _request = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
    when(service(_request.capture)).thenReturn(Future(Array[Byte]()))

    otherClientId.asCurrent {
      filter(new ThriftClientRequest(buffer.toArray, false), service)
    }

    val header = new thrift.RequestHeader
    InputBuffer.peelMessage(_request.getValue.message, header, protocolFactory)

    val clientIdContextWasSet = header.getContexts.asScala exists { c =>
      (Buf.ByteArray.Owned(c.getKey()) == ClientId.clientIdCtx.marshalId) &&
      (Buf.ByteArray.Owned(c.getValue()) == Buf.Utf8(clientId.name))
    }

    assert(header.getClient_id.getName == clientId.name)
    assert(
      clientIdContextWasSet == true,
      "expected ClientId was not set in the ClientIdContext: expected: %s, actual: %s"
        .format(clientId.name, header.getClient_id.getName)
    )
  }
}
