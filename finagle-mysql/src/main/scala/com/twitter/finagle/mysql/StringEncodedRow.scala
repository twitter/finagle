package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.MysqlBuf
import com.twitter.io.Buf

/**
 * Defines a row where the data is presumed to be encoded with the mysql
 * text-based protocol.
 * [[https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow]]
 */
private class StringEncodedRow(
  rawRow: Buf,
  val fields: IndexedSeq[Field],
  indexMap: Map[String, Int],
  ignoreUnsigned: Boolean)
    extends Row {

  /**
   * Convert the string representation of each value
   * into an appropriate Value object.
   * [[https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html]]
   */
  lazy val values: IndexedSeq[Value] = {
    val reader = MysqlBuf.reader(rawRow)
    for (field <- fields) yield {
      val charset = field.charset
      val bytes = reader.readLengthCodedBytes()
      if (bytes == null)
        NullValue
      else if (bytes.length == 0)
        EmptyValue
      else if (!MysqlCharset.isCompatible(charset))
        RawValue(field.fieldType, field.charset, isBinary = false, bytes)
      else {
        field.fieldType match {
          case Type.Tiny if isSigned(field) =>
            ByteValue(bytesToLong(bytes).toByte)
          case Type.Tiny =>
            ShortValue(bytesToLong(bytes).toShort)
          case Type.Short if isSigned(field) =>
            ShortValue(bytesToLong(bytes).toShort)
          case Type.Short =>
            IntValue(bytesToLong(bytes).toInt)
          case Type.Int24 =>
            // both signed and unsigned fit in 32 bits
            IntValue(bytesToLong(bytes).toInt)
          case Type.Long if isSigned(field) =>
            IntValue(bytesToLong(bytes).toInt)
          case Type.Long =>
            LongValue(bytesToLong(bytes))
          case Type.LongLong if isSigned(field) =>
            LongValue(bytesToLong(bytes))
          case Type.LongLong =>
            BigIntValue(BigInt(bytesToString(bytes, charset)))
          case Type.Float =>
            FloatValue(bytesToString(bytes, charset).toFloat)
          case Type.Double =>
            DoubleValue(bytesToString(bytes, charset).toDouble)
          case Type.Year =>
            ShortValue(bytesToLong(bytes).toShort)
          case Type.VarChar | Type.String | Type.VarString | Type.TinyBlob | Type.Blob |
              Type.MediumBlob | Type.LongBlob if !MysqlCharset.isBinary(charset) =>
            // Nonbinary strings as stored in the CHAR, VARCHAR, and TEXT data types
            StringValue(bytesToString(bytes, charset))
          case typ =>
            RawValue(typ, charset, isBinary = false, bytes)
        }
      }
    }
  }

  def indexOf(name: String): Option[Int] = indexMap.get(name)

  override protected def indexOfOrSentinel(columnName: String): Int =
    indexMap.getOrElse(columnName, -1)

  @inline
  private[this] def isSigned(field: Field): Boolean =
    ignoreUnsigned || field.isSigned

  /**
   * Numbers are encoded as byte strings.
   * E.g. `127` is `[49, 50, 55]`.
   */
  private[this] def bytesToLong(bytes: Array[Byte]): Long = {
    val isNegative = bytes(0) == '-'
    var value = 0L
    var i = if (isNegative) 1 else 0
    while (i < bytes.length) {
      val b = bytes(i) - '0'.toByte
      value *= 10L
      value += b
      i += 1
    }
    if (isNegative)
      value * -1L
    else
      value
  }

  private[this] def bytesToString(bytes: Array[Byte], charset: Short): String =
    new String(bytes, MysqlCharset(charset))

}
