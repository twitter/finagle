package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.util.ByteArrayUtil

trait Result {}

case object OK extends Result
case class Error(errorCode: Short, sqlState: String, error: String) extends Result

case class Field(
  catalog: String,
  db: String,
  table: String,
  origTable: String,
  name: String,
  origName: String,
  charset: Short,
  length: Int,
  fType: Byte,
  flags: Short,
  decimals: Byte
)
object Field {
  def decode(packet: Packet): Field = {
    var offset = 0

    def read(n: Int) = {
      val result = ByteArrayUtil.read(packet.body, offset, n)
      offset += n
      result
    }

    def readString() = {
      val result = ByteArrayUtil.readLengthCodedString(packet.body, offset)
      offset += result.size + 1 //add 1 to account for length encoding
      result
    }

    new Field(
      readString(),
      readString(),
      readString(),
      readString(),
      readString(),
      readString(),
      read(2).toShort,
      read(4).toInt,
      read(1).toByte,
      read(2).toShort,
      read(1).toByte
    )
  }
}

case class RowData(data: List[String])
object RowData {
  def decode(packet: Packet, fieldNumber: Int) = {
    var offset = 0

    def readString() = {
      val result = ByteArrayUtil.readLengthCodedString(packet.body, offset)
      offset += result.size + 1
      result
    }

    RowData(
      (0 until fieldNumber) map { _ =>
        readString()
      } toList
    )
  }
}
case class ResultSet(
  header: Byte,
  fields: List[Field],
  rawData: List[RowData]
) extends Result {

  override def toString = {
    val header = fields map { _.name } mkString("\t")
    val content = rawData map { _.data.mkString("\t") } mkString("\n")
    header + "\n" + content
  }
}
object ResultSet {
  def decode(header: Packet, fields: List[Packet], data: List[Packet]) = {
    ResultSet(
      header.body(0),
      fields map { Field.decode(_) },
      data map { RowData.decode(_, fields.size) }
    )
  }
}
case object EOF extends Result
