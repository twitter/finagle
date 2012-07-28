package com.twitter.finagle.mysql.protocol

import com.twitter.logging.Logger
import scala.math.BigInt
import java.sql.{Timestamp, Date}

trait ResultSet extends Result {
  val fields: Seq[Field]
  val rows: Seq[Row]
}

class SimpleResultSet(val fields: Seq[Field], val rows: Seq[Row]) extends ResultSet {
  override def toString = {
    val header = fields map { _.id } mkString("\t")
    val content = rows map { _.values.mkString("\t") } mkString("\n")
    header + "\n" + content
  }
}

object ResultSet {
  def decode(isBinaryEncoded: Boolean)(header: Packet, fieldPackets: Seq[Packet], rowPackets: Seq[Packet]) = {
    val fields = fieldPackets map { Field.decode(_) }

    // A name -> index map used to allow quick lookups for rows based on name.
    val indexMap = fields.map(_.id).zipWithIndex.toMap

    /**
     * Rows can be encoded as Strings or Binary depending
     * on if the ResultSet is created by a normal query or
     * a prepared statement, respectively. 
     */
    val rows = rowPackets map { p: Packet => 
      if (!isBinaryEncoded)
        new StringEncodedRow(p.body, indexMap) 
      else
        new BinaryEncodedRow(p.body, fields, indexMap)
    }

    new SimpleResultSet(fields, rows)
  }
}

/**
 * Defines an interface that allows for easily
 * decoding a row (Array[Byte]) into its appropriate
 * values.
 */
trait Row {
  val values: IndexedSeq[Any]
  def findColumnIndex(columnName: String): Option[Int]
  def getColumnValue(index: Int) = values(index)

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

  def getTimestamp(columnIndex: Option[Int]): Option[Timestamp]
  def getTimestamp(columnName: String): Option[Timestamp] =
    getTimestamp(findColumnIndex(columnName))

  def getDatetime(columnIndex: Option[Int]): Option[Timestamp]
  def getDatetime(columnName: String): Option[Timestamp] =
    getDatetime(findColumnIndex(columnName))

  def getDate(columnIndex: Option[Int]): Option[Date]
  def getDate(columnName: String): Option[Date] =
    getDate(findColumnIndex(columnName))
}

class StringEncodedRow(row: Array[Byte], indexMap: Map[String, Int]) extends Row {
  val br = new BufferReader(row)
  val values: IndexedSeq[String] = (0 until indexMap.size) map { _ => br.readLengthCodedString() }

  def findColumnIndex(name: String) = indexMap.get(name)

  /**
   * The readLengthCodedString method returns null when
   * a SQL NULL value is returned from the database. This
   * translates null into Option.
   */
  private[this] def getValue(index: Int): Option[String] = Option(values(index))

  def getString(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value

  def getBoolean(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value == "1"

  def getByte(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toByte

  def getShort(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toShort

  def getInt(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toInt

  def getLong(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toLong

  def getFloat(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toFloat

  def getDouble(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield value.toDouble

  def getTimestamp(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield Timestamp.valueOf(value)

  def getDatetime(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield Timestamp.valueOf(value)

  def getDate(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- getValue(idx)) yield Date.valueOf(value)
}

class BinaryEncodedRow(row: Array[Byte], fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  private[this] val log = Logger("finagle-mysql")
  val buffer = new BufferReader(row, 1) // skip first byte

  /**
   * In a binary encoded row, null values are not sent from the
   * server. Instead, the server sends a bit vector where
   * each bit corresponds to the index of the column. If the bit
   * is set, the value is null.
   */
  val nullBitmap: BigInt = {
    val len = ((fields.size + 7 + 2) / 8).toInt
    val bytesAsBigEndian = buffer.take(len).reverse
    BigInt(bytesAsBigEndian)
  }

  /**
   * Check if the bit is set. Note, the
   * first 2 bits are reserved.
   */
  def isNull(index: Int) = nullBitmap.testBit(index + 2)

  /**
   * Read values from row. Essentially coverting 
   * the row from Array[Byte] to a pseudo-heterogeneous
   * Seq that stores each column value.
   */
  val values: IndexedSeq[Option[Any]] = for(idx <- 0 until fields.size) yield {
    if (isNull(idx))
      None
    else 
      fields(idx).fieldType match {
        case Types.STRING     => Some(buffer.readLengthCodedString())
        case Types.VAR_STRING => Some(buffer.readLengthCodedString())
        case Types.VARCHAR    => Some(buffer.readLengthCodedString())
        case Types.TINY       => Some(buffer.readUnsignedByte())
        case Types.SHORT      => Some(buffer.readShort())
        case Types.INT24      => Some(buffer.readInt24())
        case Types.LONG       => Some(buffer.readInt())
        case Types.LONGLONG   => Some(buffer.readLong())
        case Types.FLOAT      => Some(buffer.readFloat())
        case Types.DOUBLE     => Some(buffer.readDouble())
        case Types.TIMESTAMP  => Some(buffer.readTimestamp())
        case Types.DATETIME   => Some(buffer.readTimestamp())
        case Types.DATE       => Some(buffer.readDate())
        
        case _ =>
          log.error("BinaryEncodedRow: Unsupported type " + 
            Integer.toHexString(fields(idx).fieldType) + ".")
          None
      }
  }

  def findColumnIndex(name: String) = indexMap.get(name)

  def getString(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[String]

  def getBoolean(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value == 1

  def getByte(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Byte]

  def getShort(columnIndex: Option[Int]) =
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Short]

  def getInt(columnIndex: Option[Int]) =
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Int]

  def getLong(columnIndex: Option[Int]) =
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Long]

  def getFloat(columnIndex: Option[Int]) =
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Float]

  def getDouble(columnIndex: Option[Int]) =
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Double]

  def getTimestamp(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Timestamp]

  def getDatetime(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Timestamp]

  def getDate(columnIndex: Option[Int]) = 
    for(idx <- columnIndex; value <- values(idx)) yield value.asInstanceOf[Date]
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
) {
  def id: String = if (name.isEmpty) origName else name
}

object Field {
  def decode(packet: Packet): Field = {
    val br = new BufferReader(packet.body)
    val catalog = br.readLengthCodedString()
    val db = br.readLengthCodedString()
    val table = br.readLengthCodedString()
    val origTable = br.readLengthCodedString()
    val name = br.readLengthCodedString()
    val origName = br.readLengthCodedString()
    br.skip(1) // filler
    val charset = br.readShort()
    val length = br.readInt()
    val fieldType = br.readUnsignedByte()
    val flags = br.readShort()
    val decimals = br.readByte()
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

