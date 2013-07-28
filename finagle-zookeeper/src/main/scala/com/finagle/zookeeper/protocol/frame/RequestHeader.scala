package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.{MessageDeserializer, RecordDeserializer, MessageSerializer, SerializableRecord}
import java.io.{InputStream, OutputStream}
import java.lang.Integer

case class RequestHeader(xid: Integer, kind: Integer) extends SerializableRecord {
// TODO: Switched from type to kind since type is a reserved word in scala
// Not sure if best decision

  override def serialize(out: MessageSerializer) {
    out.writeInteger(xid)
    out.writeInteger(kind)
  }
}

object RequestHeader extends RecordDeserializer {

  def deserialize(input: MessageDeserializer): SerializableRecord = {
    new RequestHeader(
      input.readInteger,
      input.readInteger
    )
  }
}
