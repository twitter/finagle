package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, Packet}

/**
 * A `Row` makes it easy to extract [[Value]]'s from a mysql row.
 * Specific [[Value]]'s based on mysql column name can be accessed
 * via the `apply` method.
 */
trait Row {
  /**
   * Contains a Field object for each
   * Column in the Row. The data is 0-indexed
   * so fields(0) contains the column meta-data
   * for the first column in the Row.
   */
  val fields: IndexedSeq[Field]

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

  override def toString = (fields zip values).toString
}

/**
 * Defines a row where the data is presumed to be encoded with the mysql
 * text-based protocol.
 * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow]]
 */
class StringEncodedRow(rawRow: Buffer, val fields: IndexedSeq[Field], indexMap: Map[String, Int]) extends Row {
  val buffer = BufferReader(rawRow)

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow]]
   */
  lazy val values: IndexedSeq[Value] =
    for (field <- fields) yield {
      val bytes = buffer.readLengthCodedBytes()
      if (bytes == null)
        NullValue
      else if (bytes.isEmpty)
        EmptyValue
      else if (!Charset.isCompatible(field.charset))
        RawValue(field.fieldType, field.charset, false, bytes)
      else {
        val str = new String(bytes, Charset(field.charset))
        field.fieldType match {
          case Type.Tiny       => ByteValue(str.toByte)
          case Type.Short      => ShortValue(str.toShort)
          case Type.Int24      => IntValue(str.toInt)
          case Type.Long       => IntValue(str.toInt)
          case Type.LongLong   => LongValue(str.toLong)
          case Type.Float      => FloatValue(str.toFloat)
          case Type.Double     => DoubleValue(str.toDouble)
          case Type.Year       => ShortValue(str.toShort)
          // Nonbinary strings as stored in the CHAR, VARCHAR, and TEXT data types
          case Type.VarChar | Type.String | Type.VarString |
               Type.TinyBlob | Type.Blob | Type.MediumBlob
               if !Charset.isBinary(field.charset) => StringValue(str)
          // LongBlobs indicate a sequence of bytes with length >= 2^24 which
          // can't fit into a Array[Byte]. This should be streamed and
          // support for this needs to begin at the transport layer.
          case Type.LongBlob => throw new UnsupportedOperationException("LongBlob is not supported!")
          case typ => RawValue(typ, field.charset, false, bytes)
        }
      }
    }

  def indexOf(name: String) = indexMap.get(name)
}

/**
 * Defines a Row where the data is presumed to be encoded with the
 * mysql binary protocol.
 * [[http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html]]
 */
class BinaryEncodedRow(rawRow: Buffer, val fields: IndexedSeq[Field], indexMap: Map[String, Int]) extends Row {
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
  lazy val values: IndexedSeq[Value] =
    for ((field, idx) <- fields.zipWithIndex) yield {
      if (isNull(idx)) NullValue
      else field.fieldType match {
        case Type.Tiny        => ByteValue(buffer.readByte())
        case Type.Short       => ShortValue(buffer.readShort())
        case Type.Int24       => IntValue(buffer.readInt())
        case Type.Long        => IntValue(buffer.readInt())
        case Type.LongLong    => LongValue(buffer.readLong())
        case Type.Float       => FloatValue(buffer.readFloat())
        case Type.Double      => DoubleValue(buffer.readDouble())
        case Type.Year        => ShortValue(buffer.readShort())
        // Nonbinary strings as stored in the CHAR, VARCHAR, and TEXT data types
        case Type.VarChar | Type.String | Type.VarString |
             Type.TinyBlob | Type.Blob | Type.MediumBlob
             if !Charset.isBinary(field.charset) && Charset.isCompatible(field.charset) =>
               StringValue(buffer.readLengthCodedString(Charset(field.charset)))

        case Type.LongBlob => throw new UnsupportedOperationException("LongBlob is not supported!")
        case typ => RawValue(typ, field.charset, true, buffer.readLengthCodedBytes())
      }
    }

  def indexOf(name: String) = indexMap.get(name)
}
