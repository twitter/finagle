package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.Future
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.apache.thrift.protocol.{TMessageType, TMessage, TBinaryProtocol}
import com.twitter.finagle.tracing._
import java.net.InetSocketAddress
import com.twitter.finagle.util.ByteArrays

class ThriftServerFramedCodecSpec extends SpecificationWithJUnit with Mockito {
  val protocolFactory = new TBinaryProtocol.Factory()

  "ThriftServerTracingFilter" should {
    "read header correctly" in {
      val traceId = TraceId(Some(SpanId(1L)), None, SpanId(2L), Some(true), Flags().setDebug)
      val bufferingTracer = new BufferingTracer
      Trace.pushTracer(bufferingTracer)

      val filter = new ThriftServerTracingFilter("service", new InetSocketAddress(0), protocolFactory)

      val upgradeMsg = new OutputBuffer(protocolFactory)
      upgradeMsg().writeMessageBegin(new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
      val options = new thrift.ConnectionOptions
      options.write(upgradeMsg())
      upgradeMsg().writeMessageEnd()

      val service = mock[Service[Array[Byte], Array[Byte]]]
      service(any[Array[Byte]]) returns Future(Array[Byte]())

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

      filter(ByteArrays.concat(OutputBuffer.messageToArray(header, protocolFactory), ignoreMsg.toArray), service)

      bufferingTracer.iterator foreach { record =>
        record.traceId mustEqual traceId
        record.traceId.flags mustEqual traceId.flags
      }
    }
  }
}