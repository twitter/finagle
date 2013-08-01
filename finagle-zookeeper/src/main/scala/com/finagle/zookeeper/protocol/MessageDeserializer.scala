package com.finagle.zookeeper.protocol

import java.lang.Integer
import java.io.{DataInputStream, InputStream}
import java.nio.charset.StandardCharsets
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Incoming binary message related set of methods.
 *
 * Caution: Any method call can modify the position inside
 * the buffer after consuming a data item.
 */
trait MessageDeserializer {

  def readByte: Byte
  def readBoolean: Boolean
  def readInteger: Integer
  def readLong: Long
  def readFloat: Float
  def readDouble: Double
  def readString: String
  def readBuffer: Array[Byte]

  //TODO: Add rest of composite types
}

class BinaryMessageDeserializer(inputStream: ChannelBuffer) extends MessageDeserializer {

  def readByte: Byte = inputStream.readByte

  def readBoolean: Boolean = inputStream.readByte match {
    case 1 => true
    case 0 => false
  }

  def readInteger: Integer = inputStream.readInt

  def readLong: Long = inputStream.readLong

  def readFloat: Float = inputStream.readFloat

  def readDouble: Double = inputStream.readDouble

  def readString: String = {
    val length = inputStream.readInt

    length match {
      case -1 => throw new IllegalStateException()
      case _ => {
        val byteBuffer = new Array[Byte](length)

        inputStream.readBytes(byteBuffer, 0, length)
        new String(byteBuffer, StandardCharsets.UTF_8)
      }
    }
  }

  def readBuffer: Array[Byte] = {
    val length = inputStream.readInt

    length match {
      case -1 => throw new IllegalStateException()
      case _ => {
        val byteBuffer = new Array[Byte](length)

        inputStream.readBytes(byteBuffer, 0, length)
        byteBuffer
      }
    }
  }
}
