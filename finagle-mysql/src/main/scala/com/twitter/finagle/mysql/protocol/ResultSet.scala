package com.twitter.finagle.mysql.protocol

import com.twitter.logging.Logger
import scala.math.BigInt

/**
 * ResultSets are returned from the server for any
 * query except for INSERT, UPDATE, or ALTER TABLE.
 */
trait ResultSet extends Result {
  val fields: Seq[Field]
  val rows: Seq[Row]
}

class SimpleResultSet(val fields: Seq[Field], val rows: Seq[Row]) extends ResultSet {
  override def toString = {
    val header = fields map { _.name } mkString("\t")
    val content = rows map { _.values.mkString("\t") } mkString("\n")
    header + "\n" + content
  }
}

object ResultSet {
  def decode(isBinaryEncoded: Boolean)(header: Packet, fields: Seq[Packet], rows: Seq[Packet]) = {
    val fieldData = fields map { Field.decode(_) }

    //a Field.name -> Field.index map used to allow quick lookups for rows based on name.
    val indexMap = fieldData.map(_.name).zipWithIndex.toMap

    /**
     * Rows can be encoded as Strings or Binary depending
     * on if the ResultSet is created by a normal query or
     * a prepared statement, respectively. 
     */
    val rowData = rows map { p: Packet => 
      if(!isBinaryEncoded)
        new StringEncodedRow(p.body, indexMap) 
      else
        new BinaryEncodedRow(p.body, fieldData, indexMap)
    }

    new SimpleResultSet(fieldData, rowData)
  }
}

/**
 * Defines an interface that allows for easily
 * decoding the row (Array[Byte]) into its appropriate
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
}

class StringEncodedRow(row: Array[Byte], indexMap: Map[String, Int]) extends Row {
  val br = new BufferReader(row)
  val values: IndexedSeq[String] = (0 until indexMap.size) map { _ => br.readLengthCodedString }

  def findColumnIndex(name: String) = indexMap.get(name)

  def getString(index: Option[Int]) = index map { values(_) }
  def getBoolean(index: Option[Int]) = index map { idx => if(values(idx) == "0") false else true }
  def getByte(index: Option[Int]) = index map { values(_).toByte }
  def getShort(index: Option[Int]) = index map { values(_).toShort }
  def getInt(index: Option[Int]) = index map { values(_).toInt }
  def getLong(index: Option[Int]) = index map { values(_).toLong }
  def getFloat(index: Option[Int]) = index map { values(_).toFloat }
  def getDouble(index: Option[Int]) = index map { values(_).toDouble }
}

class BinaryEncodedRow(row: Array[Byte], fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  private val log = Logger("finagle-mysql")
  val buffer = new BufferReader(row, 1) //skip first byte

  /**
   * In a binary encoded row, null values are not sent from the
   * server. Instead, the server sends a bit vector where
   * each bit corresponds to the index of the column.
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
  val values: IndexedSeq[Any] = for(idx <- 0 until fields.size) yield {
    if(isNull(idx))
      None
    else 
      fields(idx).fieldType match {
        case Types.STRING     => buffer.readLengthCodedString
        case Types.VAR_STRING => buffer.readLengthCodedString
        case Types.VARCHAR    => buffer.readLengthCodedString
        case Types.TINY       => buffer.readUnsignedByte
        case Types.SHORT      => buffer.readShort
        case Types.INT24      => buffer.readInt24
        case Types.LONG       => buffer.readInt
        case Types.LONGLONG   => buffer.readLong
        case Types.FLOAT      => buffer.readFloat
        case Types.DOUBLE     => buffer.readDouble

        case Types.NEWDECIMAL =>
          buffer.readLengthCodedString.toDouble

        case Types.BLOB => 
          val len = buffer.readUnsignedByte
          buffer.take(len)

        case _ =>
          log.error("BinaryEncodedRow: Unsupported type " + 
            Integer.toHexString(fields(idx).fieldType) + ".")
          None
      }
  }

  def findColumnIndex(name: String) = indexMap.get(name)

  def getString(index: Option[Int]) = index map { values(_).asInstanceOf[String] }
  def getBoolean(index: Option[Int]) = index map { idx => if(values(idx) == 0) false else true }
  def getByte(index: Option[Int]) = index map { values(_).asInstanceOf[Byte] }
  def getShort(index: Option[Int]) = index map { values(_).asInstanceOf[Short] }
  def getInt(index: Option[Int]) = index map { values(_).asInstanceOf[Int] }
  def getLong(index: Option[Int]) = index map { values(_).asInstanceOf[Long] }
  def getFloat(index: Option[Int]) = index map { values(_).asInstanceOf[Float] }
  def getDouble(index: Option[Int]) = index map { values(_).asInstanceOf[Double] }
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

