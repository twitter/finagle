package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, MysqlBufReader}
import com.twitter.io.Buf

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
private class StringEncodedRow(
  rawRow: Buf,
  val fields: IndexedSeq[Field],
  indexMap: Map[String, Int],
  ignoreUnsigned: Boolean
) extends Row {
  private val reader = MysqlBuf.reader(rawRow)

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow]]
   */
  lazy val values: IndexedSeq[Value] =
    for (field <- fields) yield {
      val charset = field.charset
      val bytes = reader.readLengthCodedBytes()
      if (bytes == null)
        NullValue
      else if (bytes.isEmpty)
        EmptyValue
      else if (!Charset.isCompatible(charset))
        RawValue(field.fieldType, field.charset, false, bytes)
      else {
        val str = new String(bytes, Charset(charset))
        field.fieldType match {
          case Type.Tiny if isSigned(field) => ByteValue(str.toByte)
          case Type.Tiny => ShortValue(str.toShort)
          case Type.Short if isSigned(field) => ShortValue(str.toShort)
          case Type.Short => IntValue(str.toInt)
          case Type.Int24 if isSigned(field) => IntValue(str.toInt)
          case Type.Int24 => IntValue(str.toInt)
          case Type.Long if isSigned(field) => IntValue(str.toInt)
          case Type.Long => LongValue(str.toLong)
          case Type.LongLong if isSigned(field) => LongValue(str.toLong)
          case Type.LongLong => BigIntValue(BigInt(str))
          case Type.Float => FloatValue(str.toFloat)
          case Type.Double => DoubleValue(str.toDouble)
          case Type.Year => ShortValue(str.toShort)
          // Nonbinary strings as stored in the CHAR, VARCHAR, and TEXT data types
          case Type.VarChar | Type.String | Type.VarString | Type.TinyBlob | Type.Blob |
              Type.MediumBlob if !Charset.isBinary(charset) =>
            StringValue(str)
          // LongBlobs indicate a sequence of bytes with length >= 2^24 which
          // can't fit into a Array[Byte]. This should be streamed and
          // support for this needs to begin at the transport layer.
          case Type.LongBlob =>
            throw new UnsupportedOperationException("LongBlob is not supported!")
          case typ => RawValue(typ, charset, isBinary = false, bytes)
        }
      }
    }

  def indexOf(name: String) = indexMap.get(name)

  @inline
  private[this] def isSigned(field: Field): Boolean =
    ignoreUnsigned || field.isSigned
}

/**
 * Defines a Row where the data is presumed to be encoded with the
 * mysql binary protocol.
 * [[http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html]]
 */
private class BinaryEncodedRow(
  rawRow: Buf,
  val fields: IndexedSeq[Field],
  indexMap: Map[String, Int],
  ignoreUnsigned: Boolean
) extends Row {
  private val reader: MysqlBufReader = MysqlBuf.reader(rawRow)
  reader.skip(1)

  /**
   * In a binary encoded row, null values are not sent from the
   * server. Instead, the server sends a bit vector where
   * each bit corresponds to the index of the column. If the bit
   * is set, the value is null.
   */
  val nullBitmap: BigInt = {
    val len = ((fields.size + 7 + 2) / 8).toInt
    val bytesAsBigEndian = reader.take(len).reverse
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
   *
   * @see [[https://mariadb.com/kb/en/mariadb/resultset-row/]] for details
   *     about the binary row format.
   */
  lazy val values: IndexedSeq[Value] =
    for ((field, idx) <- fields.zipWithIndex) yield {
      if (isNull(idx)) NullValue
      else
        field.fieldType match {
          case Type.Tiny if isSigned(field) => ByteValue(reader.readByte())
          case Type.Tiny => ShortValue(reader.readUnsignedByte())
          case Type.Short if isSigned(field) => ShortValue(reader.readShortLE())
          case Type.Short => IntValue(reader.readUnsignedShortLE())
          case Type.Int24 if isSigned(field) =>
            IntValue(reader.readIntLE()) // transferred as an Int32
          case Type.Int24 =>
            // The unsigned Int24 should always fit into the first 3 bytes of signed Int32
            IntValue(reader.readIntLE())
          case Type.Long if isSigned(field) => IntValue(reader.readIntLE())
          case Type.Long => LongValue(reader.readUnsignedIntLE())
          case Type.LongLong if isSigned(field) => LongValue(reader.readLongLE())
          case Type.LongLong => BigIntValue(reader.readUnsignedLongLE())
          case Type.Float => FloatValue(reader.readFloatLE())
          case Type.Double => DoubleValue(reader.readDoubleLE())
          case Type.Year => ShortValue(reader.readShortLE())
          // Nonbinary strings as stored in the CHAR, VARCHAR, and TEXT data types
          case Type.VarChar | Type.String | Type.VarString | Type.TinyBlob | Type.Blob |
              Type.MediumBlob
              if !Charset.isBinary(field.charset) && Charset.isCompatible(field.charset) =>
            StringValue(reader.readLengthCodedString(Charset(field.charset)))

          case Type.LongBlob =>
            throw new UnsupportedOperationException("LongBlob is not supported!")
          case typ => RawValue(typ, field.charset, isBinary = true, reader.readLengthCodedBytes())
        }
    }

  def indexOf(name: String): Option[Int] = indexMap.get(name)

  @inline
  private[this] def isSigned(field: Field): Boolean =
    ignoreUnsigned || field.isSigned
}
