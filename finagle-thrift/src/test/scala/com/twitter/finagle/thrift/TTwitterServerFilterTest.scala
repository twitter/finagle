package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.util.ByteArrays
import com.twitter.io.Charsets
import com.twitter.util.{Await, Future}
import org.apache.thrift.protocol.{TMessage, TMessageType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TTwitterServerFilterTest extends FunSuite {
  val protocolFactory = Protocols.binaryFactory()

  test("handles legacy client_id headers") {
    val filter = new TTwitterServerFilter("test", protocolFactory)
    
    // Upgrade the protocol.
    val service = new Service[Array[Byte], Array[Byte]] {
      def apply(req: Array[Byte]) =
        Future.value(ClientId.current.map(_.name)
          .getOrElse("NOCLIENT")
          .getBytes(Charsets.Utf8))
    }

    val upgraded = {
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.CALL, 0))
      val options = new thrift.ConnectionOptions
      options.write(buffer())
      buffer().writeMessageEnd()
      
      filter(buffer.toArray, service)
    }
    assert(upgraded.isDefined)
    Await.result(upgraded)

    val req = {
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(
        new TMessage("testrpc", TMessageType.CALL, 0))
      buffer().writeMessageEnd()

      val header = new thrift.RequestHeader
      header.setClient_id(new thrift.ClientId("testclient"))
      val bytes = ByteArrays.concat(
        OutputBuffer.messageToArray(header, protocolFactory),
        buffer.toArray)
        
      filter(bytes, service) map { bytes =>
        // Strip the response header.
        InputBuffer.peelMessage(bytes, new thrift.ResponseHeader, protocolFactory)
      }
    }
    assert(req.isDefined)
    val rep = Await.result(req)
    val clientId = new String(rep, Charsets.Utf8)
    assert(clientId == "testclient")
  }
}
