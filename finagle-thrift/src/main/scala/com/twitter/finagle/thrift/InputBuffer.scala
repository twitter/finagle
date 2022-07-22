package com.twitter.finagle.thrift

import org.apache.thrift.TBase
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryInputTransport

private[finagle] object InputBuffer {
  def peelMessage(bytes: Array[Byte], message: TBase[_, _], protocolFactory: TProtocolFactory) = {
    val buffer = new InputBuffer(bytes, protocolFactory)
    message.read(buffer())
    buffer.remainder
  }

  def readMessageBegin(bytes: Array[Byte], protocolFactory: TProtocolFactory) = {
    val buffer = new InputBuffer(bytes, protocolFactory)
    val iprot = buffer()
    iprot.readMessageBegin()
  }
}

private[twitter] class InputBuffer(bytes: Array[Byte], protocolFactory: TProtocolFactory) {

  private[this] val memoryTransport = new TMemoryInputTransport(bytes)
  private[this] val iprot = protocolFactory.getProtocol(memoryTransport)

  def apply() = iprot

  def remainder = {
    val length = bytes.length
    memoryTransport.getBufferPosition match {
      case 0 => bytes
      case l if l == length => InputBuffers.EmptyBytes
      case position => {
        val diff = length - position
        val newBytes = new Array[Byte](diff)
        System.arraycopy(bytes, position, newBytes, 0, diff)
        newBytes
      }
    }
  }
}

object InputBuffers {
  val EmptyBytes = Array[Byte]()
}
