package com.finagle.zookeeper.protocol

import java.lang.Integer
import java.io.{DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Outgoing binary message related set of methods.
 *
 * Caution: Any method call can modify the position inside
 * the buffer after consuming a data item.
 */
trait MessageSerializer {

  // TODO: Decide if this chain calling friendly interface is OK
  def writeByte(byte: Byte): MessageSerializer
  def writeBoolean(boolean: Boolean): MessageSerializer
  def writeInteger(integer: Integer): MessageSerializer
  def writeLong(long: Long): MessageSerializer
  def writeFloat(float: Float): MessageSerializer
  def writeDouble(double: Double): MessageSerializer
  def writeString(string: String): MessageSerializer
  def writeBuffer(buffer: Array[Byte]): MessageSerializer

  def writerIndex: Int

  //TODO: Add rest of composite types
}

class BinaryMessageSerializer(outputStream: ChannelBuffer) extends MessageSerializer {

  def writerIndex = outputStream.writerIndex

  def writeByte(byte: Byte): MessageSerializer = {
    outputStream.writeByte(byte); this
  }

  // TODO: Find a more idiomatic way to do it.
  def writeBoolean(boolean: Boolean): MessageSerializer = {
    outputStream.writeByte( if (boolean) 1 else 0)
    this
  }

  def writeInteger(integer: Integer): MessageSerializer = {
    outputStream.writeInt(integer); this
  }

  def writeLong(long: Long): MessageSerializer = {
    outputStream.writeLong(long); this
  }

  def writeFloat(float: Float): MessageSerializer = {
    outputStream.writeFloat(float); this
  }

  def writeDouble(double: Double): MessageSerializer = {
    outputStream.writeDouble(double); this
  }

  // TODO: Should consider ZooKeeper stringToBuffer optimization
  def writeString(string: String): MessageSerializer = {
    outputStream.writeInt(string.length)
    outputStream.writeBytes(string.getBytes(StandardCharsets.UTF_8))
    this
  }

  def writeBuffer(buffer: Array[Byte]): MessageSerializer = {
    outputStream.writeInt(buffer.length)
    outputStream.writeBytes(buffer)
    this
  }
}
