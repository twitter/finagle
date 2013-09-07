package com.twitter.finagle.exp.swift

import com.twitter.finagle.Service
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.{Await, Future}
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ProxyTest extends FunSuite with MockitoSugar {
  test("creates valid TMessages") {
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val proxy = SwiftProxy.newClient[Test1](service)

    when(service(any[ThriftClientRequest])).thenReturn(Future.never)
    proxy.ping("hello")
    
    val arg = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
    verify(service).apply(arg.capture())
    
    val request = arg.getValue()
    assert(!request.oneway)

    val in = new TBinaryProtocol(
      new TMemoryInputTransport(request.message))
    val msg = in.readMessageBegin()
    assert(msg.`type` === TMessageType.CALL)
    in.readStructBegin()
    val f = in.readFieldBegin()
    assert(f.`type` === TType.STRING)
    assert(f.`id` === 1)
    in.readFieldEnd()
    assert(in.readFieldBegin().`type` === TType.STOP)
    in.readFieldEnd()
    in.readStructEnd()
  }
  
  test("parses replies") {
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val proxy = SwiftProxy.newClient[Test1](service)
    
    val reply = Util.newMessage("ping", TMessageType.REPLY) { out =>
      out.writeFieldBegin(new TField("success", TType.STRING, 0))
      out.writeString("YAY")
    }
    when(service(any[ThriftClientRequest])).thenReturn(Future.value(reply))
    
    assert(Await.result(proxy.ping("OH HEY")) === "YAY")
  }
}
