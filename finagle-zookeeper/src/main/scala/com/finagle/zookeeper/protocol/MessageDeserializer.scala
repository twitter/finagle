package com.finagle.zookeeper.protocol

import java.lang.Integer

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
