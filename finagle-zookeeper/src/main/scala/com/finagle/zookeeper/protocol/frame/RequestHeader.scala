package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.SerializableRecord
import java.io.OutputStream

class RequestHeader extends SerializableRecord {
  /**
   * Output the record to a byte representation down the wire.
   * @param out The wire output stream
   */
  def serialize(out: OutputStream) {}
}
