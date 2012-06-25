package com.twitter.finagle.thrift

import org.apache.thrift.TBase
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryInputTransport

private[thrift] object InputBuffer {
  private[thrift] val protocolFactory = new TBinaryProtocol.Factory()

  def peelMessage(bytes: Array[Byte], message: TBase[_, _]) = {
    val buffer = new InputBuffer(bytes)
    message.read(buffer())
    buffer.remainder
  }

  def readMessageBegin(bytes: Array[Byte]) = {
    val buffer = new InputBuffer(bytes)
    val iprot = buffer()
    iprot.readMessageBegin()
  }
}

private[thrift] class InputBuffer(bytes: Array[Byte]) {
  import InputBuffer._

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