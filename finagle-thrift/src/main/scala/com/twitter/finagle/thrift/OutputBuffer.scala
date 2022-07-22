package com.twitter.finagle.thrift

import org.apache.thrift.TBase
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryBuffer

/**
 * OutputBuffers are convenient ways of getting at TProtocols for
 * output to byte arrays
 */
private[finagle] object OutputBuffer {
  def messageToArray(message: TBase[_, _], protocolFactory: TProtocolFactory) = {
    val buffer = new OutputBuffer(protocolFactory)
    message.write(buffer())
    buffer.toArray
  }
}

private[twitter] class OutputBuffer(protocolFactory: TProtocolFactory) {
  private[this] val memoryBuffer = new TMemoryBuffer(512)
  private[this] val oprot = protocolFactory.getProtocol(memoryBuffer)

  def apply() = oprot

  def toArray = {
    oprot.getTransport().flush()
    java.util.Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length())
  }
}
