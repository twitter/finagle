package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.{MessageDeserializer, RecordDeserializer, MessageSerializer, SerializableRecord}
import java.io.OutputStream

case class ConnectRequest(protocolVersion: Int,
                          lastZXIDSeen: Long,
                          timeOut: Int,
                          sessionID: Long,
                          password: Array[Byte]
                           ) extends SerializableRecord{


  def serialize(out: MessageSerializer) {
    out.writeInteger(protocolVersion)
    out.writeLong(lastZXIDSeen)
    out.writeInteger(timeOut)
    out.writeLong(sessionID)
    out.writeBuffer(password)
  }

}

object ConnectRequest extends RecordDeserializer {
  def deserialize(input: MessageDeserializer): SerializableRecord = {
    new ConnectRequest(
      input.readInteger,
      input.readLong,
      input.readInteger,
      input.readLong,
      input.readBuffer
    )
  }
}
