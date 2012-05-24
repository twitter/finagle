package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.{Util, Packet}


trait Result {}

case object OK extends Result
case object Error extends Result

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
    var i = 0
    def read(n: Int) = {
      val result = Util.read(packet.data, i, n)
      i += n
      result
    }
    def readString() = {
      val result = Util.readLengthCodedString(packet.data, i)
      i += result.size + 1
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
    var i = 0
    def readString() = {
      val result = Util.readLengthCodedString(packet.data, i)
      i += result.size + 1
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
      header.data(0),
      fields map { Field.decode(_) },
      data map { RowData.decode(_, fields.size) }
    )
  }
}
case object EOF extends Result
