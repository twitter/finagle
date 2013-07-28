package com.finagle.zookeeper.protocol.frame

import com.finagle.zookeeper.protocol.MessageDeserializer
import com.finagle.zookeeper.protocol.RecordDeserializer
import com.finagle.zookeeper.protocol.MessageSerializer
import com.finagle.zookeeper.protocol.SerializableRecord
import com.finagle.zookeeper.protocol.{MessageDeserializer, RecordDeserializer, MessageSerializer, SerializableRecord}
import java.io.{InputStream, OutputStream}
import java.lang.Integer

case class ReplyHeader(xid: Integer, zxid:Long, err: Integer) extends SerializableRecord {

  def serialize(out: MessageSerializer) {
    out.writeInteger(xid).writeLong(zxid).writeInteger(err)
  }
}

object ReplyHeader extends RecordDeserializer {

  def deserialize(input: MessageDeserializer): SerializableRecord = {
    new ReplyHeader(
      input.readInteger,
      input.readLong,
      input.readInteger
    )
  }
}

