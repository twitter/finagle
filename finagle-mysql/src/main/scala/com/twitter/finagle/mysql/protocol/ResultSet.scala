package com.twitter.finagle.mysql.protocol

import com.twitter.logging.Logger
import java.sql.{Time, Timestamp, Date, SQLException}

/**
 * ResultSets are returned from the server for any
 * query except for INSERT, UPDATE, or ALTER TABLE.
 */
trait ResultSet extends Result {
  def findColumnIndex(columnName: String): Option[Int]
  def next(): Boolean

  def getString(columnIndex: Option[Int]): Option[String]
  def getString(columnName: String): Option[String] =
    getString(findColumnIndex(columnName))

  def getBoolean(columnIndex: Option[Int]): Option[Boolean]
  def getBoolean(columnName: String): Option[Boolean] = 
    getBoolean(findColumnIndex(columnName))

  def getByte(columnIndex: Option[Int]): Option[Byte]
  def getByte(columnName: String): Option[Byte] = 
    getByte(findColumnIndex(columnName))

  def getShort(columnIndex: Option[Int]): Option[Short]
  def getShort(columnName: String): Option[Short] =
    getShort(findColumnIndex(columnName))

  def getInt(columnIndex: Option[Int]): Option[Int]
  def getInt(columnName: String): Option[Int] = 
    getInt(findColumnIndex(columnName))

  def getLong(columnIndex: Option[Int]): Option[Long]
  def getLong(columnName: String): Option[Long] =
    getLong(findColumnIndex(columnName))

  def getFloat(columnIndex: Option[Int]): Option[Float]
  def getFloat(columnName: String): Option[Float] =
    getFloat(findColumnIndex(columnName))

  def getDouble(columnIndex: Option[Int]): Option[Double]
  def getDouble(columnName: String): Option[Double] =
    getDouble(findColumnIndex(columnName))
}

class DefaultResultSet(fields: List[Field], rows: List[Row]) extends ResultSet {
    private var rowIndex: Int = -1
    private var currentRow: Option[Row] = None
    private val indexMap = fields.map(_.name).zipWithIndex.toMap

    def findColumnIndex(name: String) = indexMap.get(name)

    def getString(columnIndex: Option[Int]) = 
     for(idx <- columnIndex; row <- currentRow) yield row.getString(idx)

    def getBoolean(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getBoolean(idx)

    def getByte(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getByte(idx)

    def getShort(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getShort(idx)

    def getInt(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getInt(idx)

    def getLong(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getLong(idx)

    def getFloat(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getFloat(idx)

    def getDouble(columnIndex: Option[Int]) =
      for(idx <- columnIndex; row <- currentRow) yield row.getDouble(idx)

    def next(): Boolean = {
      rowIndex += 1
      if(rowIndex < rows.size) {
        currentRow = Some(rows(rowIndex))
        true
      } else {
        currentRow = None
        false
      }
    }

    override def toString = {
      val header = fields map { _.name } mkString("\t")
      val content = rows map { _.values.mkString("\t") } mkString("\n")
      header + "\n" + content
    }
}

object ResultSet {
  def decode(isBinaryEncoded: Boolean)(header: Packet, fields: List[Packet], rows: List[Packet]) = {
    val columnData = fields map { Field.decode(_) }
    val rowData = rows map { p: Packet => Row(p.body, columnData, isBinaryEncoded) }
    new DefaultResultSet(columnData, rowData)
  }
}

/**
 * Represents a row of data in a ResultSet.
 * Rows can be encoded as Strings or Binary depending
 * on if the ResultSet is created by a normal query or
 * a prepared statement, respectively. 
 */
trait Row {
  val values: IndexedSeq[Any]
  def getColumnValue(index: Int) = values(index)
  def getString(index: Int): String
  def getBoolean(index: Int): Boolean
  def getByte(index: Int): Byte
  def getShort(index: Int): Short
  def getInt(index: Int): Int
  def getLong(index: Int): Long
  def getFloat(index: Int): Float
  def getDouble(index: Int): Double
}

class StringEncodedRow(row: Array[Byte], fields: List[Field]) extends Row {
  val br = new BufferReader(row)
  val values: IndexedSeq[String] = (0 until fields.size) map { _ => br.readLengthCodedString }

  def getString(index: Int) = values(index)
  def getBoolean(index: Int) = if(values(index) == "0") false else true
  def getByte(index: Int) = values(index).toByte
  def getShort(index: Int) = values(index).toShort
  def getInt(index: Int) = values(index).toInt
  def getLong(index: Int) = values(index).toLong
  def getFloat(index: Int) = values(index).toFloat
  def getDouble(index: Int) = values(index).toDouble
}

class BinaryEncodedRow(row: Array[Byte], fields: List[Field]) extends Row {
  private val log = Logger("finagle-mysql")
  val buffer = new BufferReader(row, 1) //skip first byte

  /**
   * In a binary encoded row, null values are not sent from the
   * server. Instead, the server sends a bit vector where
   * each bit corresponds to the index of the column. Note, the
   * first 2 bits are reserved.
   */
  val nullBitmap: Int = {
    val len = ((fields.size + 7 + 2) / 8).toInt
    val bits = buffer.read(len).toInt
    println("Null Bit Map: " + Integer.toBinaryString(bits))
    bits
  }

  def isNull(index: Int) = ((nullBitmap >> (index+2)) & 1) == 1

  /**
   * Read values from row. Essentially coverting 
   * the row from Array[Byte] to a pseudo-hetergenous
   * Seq that stores each columns value.
   */
  val values: IndexedSeq[Any] = for(idx <- 0 until fields.size) yield {
    if(isNull(idx))
      None
    else 
      fields(idx).fieldType match {
        case Field.FIELD_TYPE_STRING => buffer.readLengthCodedString
        case Field.FIELD_TYPE_VAR_STRING => buffer.readLengthCodedString
        case Field.FIELD_TYPE_TINY => buffer.readUnsignedByte
        case Field.FIELD_TYPE_SHORT => buffer.readShort
        case Field.FIELD_TYPE_INT24 => buffer.readInt24
        case Field.FIELD_TYPE_LONG => buffer.readInt
        case Field.FIELD_TYPE_LONGLONG => buffer.readLong
        case Field.FIELD_TYPE_FLOAT => buffer.readFloat
        case Field.FIELD_TYPE_DOUBLE => buffer.readDouble

        case Field.FIELD_TYPE_NEWDECIMAL =>
          buffer.readLengthCodedString.toDouble

        case Field.FIELD_TYPE_BLOB => 
          val len = buffer.readUnsignedByte
          buffer.take(len)

        case _ =>
          log.error("BinaryEncodedRow: Unsupported type " + 
            Integer.toHexString(fields(idx).fieldType) + ".")
          None
      }
  }

  def getString(index: Int) = values(index).asInstanceOf[String]
  def getBoolean(index: Int) = if(values(index) == 0) false else true
  def getByte(index: Int) = values(index).asInstanceOf[Byte]
  def getShort(index: Int) = values(index).asInstanceOf[Short]
  def getInt(index: Int) = values(index).asInstanceOf[Int]
  def getLong(index: Int) = values(index).asInstanceOf[Long]
  def getFloat(index: Int) = values(index).asInstanceOf[Float]
  def getDouble(index: Int) = values(index).asInstanceOf[Double]

}

object Row {
  def apply(data: Array[Byte], fields: List[Field], isBinaryEncoded: Boolean): Row = {
    if(isBinaryEncoded)
      new BinaryEncodedRow(data, fields)
    else
      new StringEncodedRow(data, fields)
  }
}

/**
 * A ResultSet contains a Field packet for each column.
 */
case class Field(
  catalog: String,
  db: String,
  table: String,
  origTable: String,
  name: String,
  origName: String,
  charset: Short,
  length: Int,
  fieldType: Int,
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
    val catalog = br.readLengthCodedString
    val db = br.readLengthCodedString
    val table = br.readLengthCodedString
    val origTable = br.readLengthCodedString
    val name = br.readLengthCodedString
    val origName = br.readLengthCodedString
    br.skip(1) //filler
    val charset = br.readShort
    val length = br.readInt
    val fieldType = br.readUnsignedByte
    val flags = br.readShort
    val decimals = br.readByte
    new Field(
      catalog,
      db,
      table,
      origTable,
      name,
      origName,
      charset,
      length,
      fieldType,
      flags,
      decimals
    )
  }
}

