package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.ByteArrays
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ThriftServerTracingFilterTest extends AnyFunSuite with MockitoSugar {
  val protocolFactory = new TBinaryProtocol.Factory()

  test("ThriftServerTracingFilter read header correctly") {
    val traceId = TraceId(Some(SpanId(1L)), None, SpanId(2L), Some(true), Flags().setDebug)
    val bufferingTracer = new BufferingTracer
    Trace.letTracer(bufferingTracer) {
      val filter = new TTwitterServerFilter("service", protocolFactory)

      val upgradeMsg = new OutputBuffer(protocolFactory)
      upgradeMsg().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0)
      )
      val options = new thrift.ConnectionOptions
      options.write(upgradeMsg())
      upgradeMsg().writeMessageEnd()

      val service = mock[Service[Array[Byte], Array[Byte]]]
      when(service(ArgumentMatchers.any[Array[Byte]])).thenReturn(Future(Array[Byte]()))

      // now finagle knows we can handle the headers
      filter(upgradeMsg.toArray, service)

      // let's create a header
      val header = new thrift.RequestHeader
      header.setSpan_id(2L)
      header.setTrace_id(1L)
      header.setSampled(true)
      header.setFlags(1L)

      val ignoreMsg = new OutputBuffer(protocolFactory)
      ignoreMsg().writeMessageBegin(new TMessage("ignoreme", TMessageType.CALL, 0))
      new thrift.ConnectionOptions().write(ignoreMsg())
      ignoreMsg().writeMessageEnd()

      filter(
        ByteArrays.concat(OutputBuffer.messageToArray(header, protocolFactory), ignoreMsg.toArray),
        service
      )

      bufferingTracer.iterator foreach { record =>
        assert(record.traceId == traceId)
        assert(record.traceId.flags == traceId.flags)
      }
    }
  }
}
