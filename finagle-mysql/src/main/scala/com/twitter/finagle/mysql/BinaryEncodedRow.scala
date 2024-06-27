package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.MysqlBuf
import com.twitter.finagle.mysql.transport.MysqlBufReader
import com.twitter.io.Buf

/**
 * Defines a Row where the data is presumed to be encoded with the
 * mysql binary protocol.
 * [[https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html]]
 */
private class BinaryEncodedRow(
  rawRow: Buf,
  val fields: IndexedSeq[Field],
  indexMap: Map[String, Int],
  ignoreUnsigned: Boolean)
    extends Row {
  private val reader: MysqlBufReader = MysqlBuf.reader(rawRow)
  reader.skip(1)

  /**
   * In a binary encoded row, null values are not sent from the
   * server. Instead, the server sends a bit vector where
   * each bit corresponds to the index of the column. If the bit
   * is set, the value is null.
   */
  val nullBitmap: BigInt = {
    val len = (fields.size + 7 + 2) / 8
    val bytesAsBigEndian = reader.take(len).reverse
    BigInt(bytesAsBigEndian)
  }

  /**
   * Check if the bit is set. Note, the
   * first 2 bits are reserved.
   */
  def isNull(index: Int): Boolean = nullBitmap.testBit(index + 2)

  /**
   * Convert the binary representation of each value
   * into an appropriate Value object.
   *
   * @see [[https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html]] for details
   *     about the binary row format.
   */
  lazy val values: IndexedSeq[Value] = {
    val vs = new Array[Value](fields.length)
    var idx = 0
    while (idx < fields.length) {
      vs(idx) =
        if (isNull(idx)) NullValue
        else {
          val field = fields(idx)
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
                Type.MediumBlob | Type.LongBlob
                if !MysqlCharset.isBinary(field.charset) && MysqlCharset.isCompatible(
                  field.charset
                ) =>
              StringValue(reader.readLengthCodedString(MysqlCharset(field.charset)))
            case typ => RawValue(typ, field.charset, isBinary = true, reader.readLengthCodedBytes())
          }
        }
      idx += 1
    }
    vs
  }

  def indexOf(name: String): Option[Int] = indexMap.get(name)

  override protected def indexOfOrSentinel(columnName: String): Int =
    indexMap.getOrElse(columnName, -1)

  @inline
  private[this] def isSigned(field: Field): Boolean =
    ignoreUnsigned || field.isSigned
}
