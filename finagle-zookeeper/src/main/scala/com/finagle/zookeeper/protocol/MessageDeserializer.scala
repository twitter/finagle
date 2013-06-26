package com.finagle.zookeeper.protocol

import java.lang.Integer
import java.io.{DataInputStream, InputStream}
import java.nio.charset.StandardCharsets

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
  def readString: Option[String]
  def readBuffer: Option[Array[Byte]]

  //TODO: Add rest of composite types
}

class BinaryMessageDeserializer(inputStream: InputStream) extends MessageDeserializer {

  private val streamWrapper = new DataInputStream(inputStream)

  def readByte: Byte = streamWrapper.readByte

  def readBoolean: Boolean = streamWrapper.readBoolean

  def readInteger: Integer = streamWrapper.readInt

  def readLong: Long = streamWrapper.readLong

  def readFloat: Float = streamWrapper.readFloat

  def readDouble: Double = streamWrapper.readDouble

  def readString: Option[String] = {
    val length = streamWrapper.readInt

    length match {
      case -1 => None
      case _ => {
        val byteBuffer = new Array[Byte](length)

        streamWrapper.readFully(byteBuffer)
        new Some(new String(byteBuffer, StandardCharsets.UTF_8))
      }
    }
  }

  def readBuffer: Option[Array[Byte]] = {
    val length = streamWrapper.readInt

    length match {
      case -1 => None
      case _ => {
        val byteBuffer = new Array[Byte](length)

        streamWrapper.readFully(byteBuffer)
        new Some(byteBuffer)
      }
    }
  }
}
