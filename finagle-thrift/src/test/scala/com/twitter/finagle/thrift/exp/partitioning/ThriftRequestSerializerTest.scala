package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.thrift.thriftjava.Echo
import com.twitter.finagle.{Stack, Thrift}
import com.twitter.scrooge.{ThriftStruct, ThriftStructIface}
import org.apache.thrift.protocol.{TJSONProtocol, TMessage, TMessageType, TProtocolUtil, TType}
import org.apache.thrift.transport.TMemoryInputTransport
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ThriftRequestSerializerTest extends AnyFunSuite with MockitoSugar {

  val protocolFactory = new TJSONProtocol.Factory()
  val stackParams = Stack.Params.empty + Thrift.param.ProtocolFactory(protocolFactory)

  val requestSerializer = new ThriftRequestSerializer(stackParams)

  test("ThriftRequestSerializer serialize 2 messages") {
    val ts = mock[ThriftStruct]
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

  test("ThriftRequestSerializer serialize java request types") {
    val javaRequest = new Echo.echo_args("hi")
    val thriftClientRequest1 = requestSerializer.serialize("messageName1", javaRequest, false)
    val thriftClientRequest2 = requestSerializer.serialize("messageName2", javaRequest, false)

    val deserializer: Array[Byte] => TMessage = bytes => {
      val inputTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(inputTransport)
      val msg = iprot.readMessageBegin()
      TProtocolUtil.skip(iprot, TType.STRUCT)
      iprot.readMessageEnd()
      msg
    }

    assert(deserializer(thriftClientRequest1.message).name == "messageName1")
    assert(deserializer(thriftClientRequest1.message).`type` == TMessageType.CALL)

    assert(deserializer(thriftClientRequest2.message).name == "messageName2")
    assert(deserializer(thriftClientRequest2.message).`type` == TMessageType.CALL)
  }

  test("ThriftRequestSerializer cannot serialize illegal request type") {
    class A extends ThriftStructIface
    val aRequest = new A
    intercept[IllegalArgumentException](
      requestSerializer.serialize("messageName1", aRequest, false))
  }

}
