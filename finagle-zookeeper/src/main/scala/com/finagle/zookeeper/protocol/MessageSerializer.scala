package com.finagle.zookeeper.protocol

import java.lang.Integer
import java.io.{DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets

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

  //TODO: Add rest of composite types
}

class BinaryMessageSerializer(outputStream: OutputStream) extends MessageSerializer {

  private val streamWrapper = new DataOutputStream(outputStream)

  def writeByte(byte: Byte): MessageSerializer = {
    streamWrapper.writeByte(byte); this
  }

  def writeBoolean(boolean: Boolean): MessageSerializer = {
    streamWrapper.writeBoolean(boolean); this
  }

  def writeInteger(integer: Integer): MessageSerializer = {
    streamWrapper.writeInt(integer); this
  }

  def writeLong(long: Long): MessageSerializer = {
    streamWrapper.writeLong(long); this
  }

  def writeFloat(float: Float): MessageSerializer = {
    streamWrapper.writeFloat(float); this
  }

  def writeDouble(double: Double): MessageSerializer = {
    streamWrapper.writeDouble(double); this
  }

  // TODO: Should consider ZooKeeper stringToBuffer optimization
  def writeString(string: String): MessageSerializer = {
    streamWrapper.writeInt(string.length)
    streamWrapper.write(string.getBytes(StandardCharsets.UTF_8))
    this
  }

  def writeBuffer(buffer: Array[Byte]): MessageSerializer = {
    streamWrapper.writeInt(buffer.length)
    streamWrapper.write(buffer)
    this
  }
}
