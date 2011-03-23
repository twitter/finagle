package com.twitter.finagle.thrift

/**
 * OutputBuffers are convenient ways of getting at TProtocols for
 * output to byte arrays
 */

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryBuffer
import org.apache.thrift.TBase

private[thrift] object OutputBuffer {
  private[thrift] val protocolFactory = new TBinaryProtocol.Factory()

  def messageToArray(message: TBase[_, _]) = {
    val buffer = new OutputBuffer
    message.write(buffer())
    buffer.toArray
  }
}

private[thrift] class OutputBuffer {
  import OutputBuffer._

  private[this] val memoryBuffer = new TMemoryBuffer(512)
  private[this] val oprot = protocolFactory.getProtocol(memoryBuffer)

  def apply() = oprot

  def toArray = {
    oprot.getTransport().flush()
    java.util.Arrays.copyOfRange(
      memoryBuffer.getArray(), 0, memoryBuffer.length())
  }
}

