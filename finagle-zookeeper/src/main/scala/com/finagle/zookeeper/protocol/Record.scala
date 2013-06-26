package com.finagle.zookeeper.protocol

/**
 * Every unit of communication between a client and a server should
 * expose a method of serialization to a byte array
 */
trait Record {

  /**
   * Output the record to a byte array for being sent down the wire.
   * @return
   */
  def serialize: Array[Byte]
}
