package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, Packet}

/**
 * Defines an interface that allows for easily
 * reading the values in a row.
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

/**
 * Defines a row where the data is presumed to be encoded with the mysql
 * text-based protocol.
 * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow]]
 */
class StringEncodedRow(rawRow: Buffer, val fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  val br = BufferReader(rawRow)

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   */
  val values: IndexedSeq[Value] =
    for (field: Field <- fields.toIndexedSeq) yield {
      Value(field.fieldType, br.readLengthCodedString(Charset(field.charset)))
    }

  def indexOf(name: String) = indexMap.get(name)
}

/**
 * Defines a Row where the data is presumed to be encoded with the
 * mysql binary protocol.
 * [[http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html]]
 */
class BinaryEncodedRow(rawRow: Buffer, val fields: Seq[Field], indexMap: Map[String, Int]) extends Row {
  val buffer = BufferReader(rawRow, offset = 1)

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
  val values: IndexedSeq[Value] =
    for ((field, idx) <- fields.toIndexedSeq.zipWithIndex) yield {
      if (isNull(idx))
        NullValue
      else
        Value(field.fieldType, buffer, Charset(field.charset))
    }

  def indexOf(name: String) = indexMap.get(name)
}