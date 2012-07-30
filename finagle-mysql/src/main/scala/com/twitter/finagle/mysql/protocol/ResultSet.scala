package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.ClientError
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
        new StringEncodedRow(p.body, fields, indexMap) 
      else
        new BinaryEncodedRow(p.body, fields, indexMap)
    }

    new SimpleResultSet(fields, rows)
  }
}

/**
 * Defines an interface that allows for easily
 * decoding a row into its appropriate values.
 */
trait Row {
  val values: IndexedSeq[Value]
  def findColumnIndex(columnName: String): Option[Int]

  def valueOf(columnIndex: Option[Int]): Option[Value] =
    for(idx <- columnIndex) yield values(idx)

  def valueOf(columnName: String): Option[Value] = 
    valueOf(findColumnIndex(columnName))
}

class StringEncodedRow(row: Array[Byte], fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  val br = new BufferReader(row)

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   */
  val values: IndexedSeq[Value] = for(idx <- 0 until fields.size) yield {
    val fieldType = fields(idx).fieldType
    val value = br.readLengthCodedString()
    if (value == null)
      NullValue
    else if (value.isEmpty)
      EmptyValue
    else
      fieldType match {
        case TypeCodes.STRING     => StringValue(value)
        case TypeCodes.VAR_STRING => StringValue(value)
        case TypeCodes.VARCHAR    => StringValue(value)
        case TypeCodes.TINY       => ByteValue(value.toByte)
        case TypeCodes.SHORT      => ShortValue(value.toShort)
        case TypeCodes.INT24      => IntValue(value.toInt)
        case TypeCodes.LONG       => IntValue(value.toInt)
        case TypeCodes.LONGLONG   => LongValue(value.toLong)
        case TypeCodes.FLOAT      => FloatValue(value.toFloat)
        case TypeCodes.DOUBLE     => DoubleValue(value.toDouble)
        case TypeCodes.TIMESTAMP  => Value.toTimestampValue(value) 
        case TypeCodes.DATETIME   => Value.toTimestampValue(value)
        case TypeCodes.DATE       => Value.toDateValue(value)
        case _                    => RawValue(value)
      }
  }

  def findColumnIndex(name: String) = indexMap.get(name)
}

class BinaryEncodedRow(row: Array[Byte], fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
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
   * Convert the binary representation of each value
   * into an appropriate Value object.
   */
  val values: IndexedSeq[Value] = for(idx <- 0 until fields.size) yield {
    if (isNull(idx))
      NullValue
    else 
      fields(idx).fieldType match {
        case TypeCodes.STRING      => Value.toStringValue(buffer.readLengthCodedString())
        case TypeCodes.VAR_STRING  => Value.toStringValue(buffer.readLengthCodedString())
        case TypeCodes.VARCHAR     => Value.toStringValue(buffer.readLengthCodedString())
        case TypeCodes.TINY        => ByteValue(buffer.readByte())
        case TypeCodes.SHORT       => ShortValue(buffer.readShort())
        case TypeCodes.INT24       => IntValue(buffer.readInt24())
        case TypeCodes.LONG        => IntValue(buffer.readInt())
        case TypeCodes.LONGLONG    => LongValue(buffer.readLong())
        case TypeCodes.FLOAT       => FloatValue(buffer.readFloat())
        case TypeCodes.DOUBLE      => DoubleValue(buffer.readDouble())
        case TypeCodes.TIMESTAMP   => TimestampValue(buffer.readTimestamp())
        case TypeCodes.DATETIME    => TimestampValue(buffer.readTimestamp())
        case TypeCodes.DATE        => DateValue(buffer.readDate())

        // Unsure about the binary representation for these.
        case TypeCodes.NEWDATE     => throw new ClientError("Unsupported field type: " + TypeCodes.NEWDATE)
        case TypeCodes.ENUM        => throw new ClientError("Unsupported field type: " + TypeCodes.ENUM)
        case TypeCodes.SET         => throw new ClientError("Unsupported field type: " + TypeCodes.SET)

        // The rest can safely be read as a length coded set of bytes.
        case _                     => RawBinaryValue(buffer.readLengthCodedBytes())
      }
  }

  def findColumnIndex(name: String) = indexMap.get(name)
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

