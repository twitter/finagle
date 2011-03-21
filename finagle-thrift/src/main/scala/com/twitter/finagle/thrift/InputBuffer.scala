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
}

private[thrift] class InputBuffer(bytes: Array[Byte]) {
  import InputBuffer._

  private[this] val memoryTransport = new TMemoryInputTransport(bytes)
  private[this] val iprot = protocolFactory.getProtocol(memoryTransport)

  def apply() = iprot

  def remainder = bytes drop memoryTransport.getBufferPosition
}
