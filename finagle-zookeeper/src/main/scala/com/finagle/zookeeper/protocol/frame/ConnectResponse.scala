package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.{MessageDeserializer, RecordDeserializer, MessageSerializer, SerializableRecord}

case class ConnectResponse(protocolVersion: Int,
                           timeOut: Int,
                           sessionID: Long,
                           password: Array[Byte]
                            ) extends SerializableRecord{


  def serialize(out: MessageSerializer) {
    out.writeInteger(protocolVersion)
    out.writeInteger(timeOut)
    out.writeLong(sessionID)
    out.writeBuffer(password)
  }

}

object ConnectResponse extends RecordDeserializer {
  def deserialize(input: MessageDeserializer): SerializableRecord = {
    new ConnectResponse(
      input.readInteger,
      input.readInteger,
      input.readLong,
      input.readBuffer
    )
  }
}