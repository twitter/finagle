package com.finagle.zookeeper.protocol

/**
 * Incoming binary message related set of methods.
 *
 * Caution: Any method call can modify the position inside
 * the buffer after consuming a data item.
 */
trait BinaryInputMessage {

  def readByte: Byte
  def readBoolean: Boolean
  //TODO: FIx deprecation waring
  def readInteger: Integer
  def readLong: Long
  def readFloat: Float
  def readDouble: Double
  def readString: String
  def readBuffer: Array[Byte]
  //TODO: Add rest of composite types

}
