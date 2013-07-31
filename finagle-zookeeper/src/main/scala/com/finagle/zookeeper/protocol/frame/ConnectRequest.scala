package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol._
import java.io.OutputStream
import scala.Some

case class ConnectRequest(protocolVersion: Int,
                          lastZXIDSeen: Long,
                          timeOut: Int,
                          sessionID: Long,
                          password: Array[Byte]
                           ) extends SerializableRecord with ExpectsResponse {


  def serialize(out: MessageSerializer) {
    out.writeInteger(protocolVersion)
    out.writeLong(lastZXIDSeen)
    out.writeInteger(timeOut)
    out.writeLong(sessionID)
    out.writeBuffer(password)
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
      input.readBuffer
    ))
  }

  def default: ConnectRequest = new ConnectRequest(0,0,2000,0,new Array[Byte](16))
}
