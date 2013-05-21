package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.protocol.{BufferReader, Packet, Charset}
import java.sql.{Timestamp, Date => SQLDate}

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
  /**
   * Contains a Field object for each
   * Column in the Row.
   */
  val fields: Seq[Field]

  /** The values for this Row. */
  val values: IndexedSeq[Value]

  /**
   * Retrieves the index of the column with the given
   * name.
   * @param columnName name of the column.
   * @return Some(Int) if the column
   * exists with the given name. Otherwise, None.
   */
  def indexOf(columnName: String): Option[Int]

  /**
   * Retrieves the Value in the column with the
   * given name.
   * @param columnName name of the column.
   * @return Some(Value) if the column
   * exists with the given name. Otherwise, None.
   */
  def apply(columnName: String): Option[Value] =
    apply(indexOf(columnName))

  protected def apply(columnIndex: Option[Int]): Option[Value] =
    for (idx <- columnIndex) yield values(idx)

  }

class StringEncodedRow(row: Array[Byte], val fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  val br = BufferReader(row)

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   */
  val values: IndexedSeq[Value] = (for (field: Field <- fields.toIndexedSeq) yield {
    Value(field.fieldType, br.readLengthCodedString(Charset(field.charset)))
  })

  def indexOf(name: String) = indexMap.get(name)
}

class BinaryEncodedRow(row: Array[Byte], val fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  val buffer = BufferReader(row, 1) // skip first byte

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
  val values: IndexedSeq[Value] = (for ((field, idx) <- fields.toIndexedSeq.zipWithIndex) yield {
    if (isNull(idx))
      NullValue
    else
      Value(field.fieldType, buffer, Charset(field.charset))
  })

  def indexOf(name: String) = indexMap.get(name)
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
  displayLength: Int,
  fieldType: Int,
  flags: Short,
  decimals: Byte
) {
  def id: String = if (name.isEmpty) origName else name
}

object Field {
  def decode(packet: Packet): Field = {
    val br = BufferReader(packet.body)
    val bytesCatalog = br.readLengthCodedBytes()
    val bytesDb = br.readLengthCodedBytes()
    val bytesTable = br.readLengthCodedBytes()
    val bytesOrigTable = br.readLengthCodedBytes()
    val bytesName = br.readLengthCodedBytes()
    val bytesOrigName = br.readLengthCodedBytes()
    br.skip(1) // filler
    val charset = br.readShort()
    val jCharset = Charset(charset)
    val catalog = new String(bytesCatalog, jCharset)
    val db = new String(bytesDb, jCharset)
    val table = new String(bytesTable, jCharset)
    val origTable = new String(bytesOrigTable, jCharset)
    val name = new String(bytesName, jCharset)
    val origName = new String(bytesOrigName, jCharset)
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
