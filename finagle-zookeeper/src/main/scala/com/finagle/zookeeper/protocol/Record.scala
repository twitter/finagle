package com.finagle.zookeeper.protocol

import java.io.{InputStream,OutputStream}

/**
 * Every unit of communication between a client and a server should
 * expose a method of serialization to a byte array
 */
trait SerializableRecord {

  /**
   * Output the record to a byte representation down the wire.
   * @param out The wire output stream
   */
  def serialize(out: OutputStream)
}

trait RecordDeserializer {

  /**
   * Read a record from an byte input stream.
   * @param input The wire input stream
   * @return
   */
  def deserialize(input: InputStream): SerializableRecord
}
