package com.twitter.finagle.thrift.partitioning

import com.twitter.finagle.{Stack, Thrift}
import com.twitter.scrooge.ThriftStruct
import org.apache.thrift.protocol.{TJSONProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TMemoryInputTransport
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class ThriftRequestSerializerTest extends FunSuite with MockitoSugar {

  test("ThriftRequestSerializer serialize 2 messages") {
    val ts = mock[ThriftStruct]
    val protocolFactory = new TJSONProtocol.Factory()
    val stackParams = Stack.Params.empty + Thrift.param.ProtocolFactory(protocolFactory)

    val requestSerializer = new ThriftRequestSerializer(stackParams)
    val thriftClientRequest1 = requestSerializer.serialize("messageName1", ts, false)
    val thriftClientRequest2 = requestSerializer.serialize("messageName2", ts, false)

    val deserializer: Array[Byte] => TMessage = bytes => {
      val inputTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(inputTransport)
      val msg = iprot.readMessageBegin()
      iprot.readMessageEnd()
      msg
    }

    assert(deserializer(thriftClientRequest1.message).name == "messageName1")
    assert(deserializer(thriftClientRequest1.message).`type` == TMessageType.CALL)

    assert(deserializer(thriftClientRequest2.message).name == "messageName2")
    assert(deserializer(thriftClientRequest2.message).`type` == TMessageType.CALL)
  }

}
