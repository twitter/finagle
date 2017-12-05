package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.MysqlBuf
import com.twitter.io.Buf

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
