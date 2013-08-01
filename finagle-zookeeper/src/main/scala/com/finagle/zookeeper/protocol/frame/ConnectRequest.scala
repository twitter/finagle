package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol._
import java.io.OutputStream
import scala.Some

case class ConnectRequest(protocolVersion: Int,
                          lastZXIDSeen: Long,
                          timeOut: Int,
                          sessionID: Long,
                          password: Array[Byte],
                          /**
                           * Support for read only connections/ not suppported
                           * by old servers
                           */
                          canReadOnly: Option[Boolean]
                           ) extends SerializableRecord with ExpectsResponse {


  def serialize(out: MessageSerializer) {
    out.writeInteger(protocolVersion)
    out.writeLong(lastZXIDSeen)
    out.writeInteger(timeOut)
    out.writeLong(sessionID)
    out.writeBuffer(password)
    canReadOnly match {
      case Some(value) => out.writeBoolean(value)
      case None =>
    }
  }

  def responseDeserializer = new HeaderBodyDeserializer(bodyDeserializer = ConnectResponse)

}

object ConnectRequest extends RecordDeserializer {
  def deserialize(input: MessageDeserializer) = {
    Some(new ConnectRequest(
      input.readInteger,
      input.readLong,
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

  def default: ConnectRequest = new ConnectRequest(0,0,2000,0,new Array[Byte](16), Some(false))
}
