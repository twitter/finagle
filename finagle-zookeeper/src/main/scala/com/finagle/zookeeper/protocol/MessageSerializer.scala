package com.finagle.zookeeper.protocol

/**
 * Outgoing binary message related set of methods.
 *
 * Caution: Any method call can modify the position inside
 * the buffer after consuming a data item.
 */
class MessageSerializer {

  def writeByte: Byte
  def writeBoolean: Boolean
  def writeInteger: Integer
  def writeLong: Long
  def writeFloat: Float
  def writeDouble: Double
  def writeString: String
  def writeBuffer: Array[Byte]

  //TODO: Add rest of composite types

}
