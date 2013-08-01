package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.{MessageDeserializer, RecordDeserializer, MessageSerializer, SerializableRecord}

/**
 *
 * @param protocolVersion
 * @param timeOut
 * @param sessionID
 * @param password
 * @param isReadOnly
 */
case class ConnectResponse(protocolVersion: Int,
                           timeOut: Int,
                           sessionID: Long,
                           password: Array[Byte],
                           isReadOnly: Option[Boolean]
                            ) extends SerializableRecord{


  def serialize(out: MessageSerializer) {
    out.writeInteger(protocolVersion)
    out.writeInteger(timeOut)
    out.writeLong(sessionID)
    out.writeBuffer(password)
  }

}

object ConnectResponse extends RecordDeserializer {
  def deserialize(input: MessageDeserializer) = {
    Some(new ConnectResponse(
      input.readInteger,
      input.readInteger,
      input.readLong,
      input.readBuffer,
      try {
        Some(input.readBoolean)
      } catch {
        case _ => {
          logger.info("Old server, ReadOnly mode not allowed.")
          None
        }
      }
    ))
  }
}