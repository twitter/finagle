package com.twitter.finagle.mysql.protocol

trait Result

/**
 * Represents the OK Packet received from the server. It is sent
 * to indicate that a command has completed succesfully. The following
 * commands receive OK packets:
 * - COM_PING
 * - COM_QUERY (INSERT, UODATE, or ALTER TABLE)
 * - COM_REFRESH
 * - COM_REGISTER_SLAVE
 */
case class OK(affectedRows: Long, 
              insertId: Long, 
              serverStatus: Short,
              warningCount: Short,
              message: String) extends Result
object OK {
  def decode(packet: Packet) = {
    //start reading after flag byte
    val br = new BufferReader(packet.body, 1)
    new OK(
      br.readLengthCodedBinary,
      br.readLengthCodedBinary,
      br.readShort,
      br.readShort,
      new String(br.takeRest)
    )
  }
}

/**
 * Represents the Error Packet received from the server
 * and the data sent along with it.
 */
case class Error(code: Short, sqlState: String, message: String) extends Result
object Error {
  def decode(packet: Packet) = {
    //start reading after flag byte
    val br = new BufferReader(packet.body, 1)
    val code = br.readShort
    val state = new String(br.take(6))
    val msg = new String(br.takeRest)
    Error(code, state, msg)
  }
}

/** 
 * Represents and EOF result received from the server which
 * contains any warnings and the server status.
*/
case class EOF(warnings: Short, serverStatus: Short) extends Result
object EOF {
  def decode(packet: Packet) = {
    val br = new BufferReader(packet.body, 1)
    EOF(br.readShort, br.readShort)
  }
}

/* Field */
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
  val FIELD_TYPE_DECIMAL     = 0x00;
  val FIELD_TYPE_TINY        = 0x01;
  val FIELD_TYPE_SHORT       = 0x02;
  val FIELD_TYPE_LONG        = 0x03;
  val FIELD_TYPE_FLOAT       = 0x04;
  val FIELD_TYPE_DOUBLE      = 0x05;
  val FIELD_TYPE_NULL        = 0x06;
  val FIELD_TYPE_TIMESTAMP   = 0x07;
  val FIELD_TYPE_LONGLONG    = 0x08;
  val FIELD_TYPE_INT24       = 0x09;
  val FIELD_TYPE_DATE        = 0x0a;
  val FIELD_TYPE_TIME        = 0x0b;
  val FIELD_TYPE_DATETIME    = 0x0c;
  val FIELD_TYPE_YEAR        = 0x0d;
  val FIELD_TYPE_NEWDATE     = 0x0e;
  val FIELD_TYPE_VARCHAR     = 0x0f;
  val FIELD_TYPE_BIT         = 0x10;
  val FIELD_TYPE_NEWDECIMAL  = 0xf6;
  val FIELD_TYPE_ENUM        = 0xf7;
  val FIELD_TYPE_SET         = 0xf8;
  val FIELD_TYPE_TINY_BLOB   = 0xf9;
  val FIELD_TYPE_MEDIUM_BLOB = 0xfa;
  val FIELD_TYPE_LONG_BLOB   = 0xfb;
  val FIELD_TYPE_BLOB        = 0xfc;
  val FIELD_TYPE_VAR_STRING  = 0xfd;
  val FIELD_TYPE_STRING      = 0xfe;
  val FIELD_TYPE_GEOMETRY    = 0xff;

  def decode(packet: Packet): Field = {
    val br = new BufferReader(packet.body)
    new Field(
      br.readLengthCodedString,
      br.readLengthCodedString,
      br.readLengthCodedString,
      br.readLengthCodedString,
      br.readLengthCodedString,
      br.readLengthCodedString,
      br.readShort,
      br.readInt,
      br.readByte,
      br.readShort,
      br.readByte
      //TO DO: decode default value field
    )
  }
}

/* RowData */
case class RowData(data: List[String])
object RowData {
  def decode(packet: Packet, fieldNumber: Int) = {
    val br = new BufferReader(packet.body)
    RowData(
      (0 until fieldNumber) map { _ =>
        br.readLengthCodedString
      } toList
    )
  }
}

/* ResultSet */
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
