package com.twitter.finagle.exp.mysql

import java.nio.charset.{Charset => JCharset}
import com.twitter.finagle.exp.mysql.transport.Buffer

object Type {
  /**
   * MySQL type codes as enumerated here:
   * http://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
   */
  val Decimal: Short    = 0x00
  val Tiny: Short       = 0x01
  val Short: Short      = 0x02
  val Long: Short       = 0x03
  val Float: Short      = 0x04
  val Double: Short     = 0x05
  val Null: Short       = 0x06
  val Timestamp: Short  = 0x07
  val LongLong: Short   = 0x08
  val Int24: Short      = 0x09
  val Date: Short       = 0x0a
  val Time: Short       = 0x0b
  val DateTime: Short   = 0x0c
  val Year: Short       = 0x0d
  val NewDate: Short    = 0x0e
  val VarChar: Short    = 0x0f
  val Bit: Short        = 0x10
  val NewDecimal: Short = 0xf6
  val Enum: Short       = 0xf7
  val Set: Short        = 0xf8
  val TinyBlob: Short   = 0xf9
  val MediumBlob: Short = 0xfa
  val LongBlob: Short   = 0xfb
  val Blob: Short       = 0xfc
  val VarString: Short  = 0xfd
  val String: Short     = 0xfe
  val Geometry: Short   = 0xff

  /**
   * Returns the sizeof the given parameter in
   * its MySQL binary representation. If the size
   * is unknown or the value is null, 0 is returned.
   */
  private[mysql] def sizeOf(any: Any): Int = any match {
    case b: Boolean     => 1
    case b: Byte        => 1
    case s: Short       => 2
    case i: Int         => 4
    case l: Long        => 8
    case f: Float       => 4
    case d: Double      => 8
    case t: java.sql.Timestamp    => 12
    case d: java.sql.Date         => 5
    case d: java.util.Date        => 12
    case s: String =>
      val bytes = s.getBytes(Charset.defaultCharset)
      Buffer.sizeOfLen(bytes.size) + bytes.size
    case b: Array[Byte] =>
      Buffer.sizeOfLen(b.size) + b.size
    case RawValue(_, _, true, b)  =>
      Buffer.sizeOfLen(b.size) + b.size
    case StringValue(s) =>
      val bytes = s.getBytes(Charset.defaultCharset)
      Buffer.sizeOfLen(bytes.size) + bytes.size
    case ByteValue(_) => 1
    case ShortValue(_) => 2
    case IntValue(_) => 4
    case LongValue(_) => 8
    case FloatValue(_) => 4
    case DoubleValue(_) => 8
    case NullValue => 0
    case null => 0
    // This is safe because unknown
    // values are serialized as null.
    case _ => 0
  }

  /**
   * Retrieves the MySQL type code for the
   * given parameter. If the parameter type
   * mapping is unknown -1 is returned.
   */
  private[mysql] def getCode(any: Any): Short = any match {
    // primitives
    case s: String  => VarChar
    case b: Boolean => Tiny
    case b: Byte    => Tiny
    case s: Short   => Short
    case i: Int     => Long
    case l: Long    => LongLong
    case f: Float   => Float
    case d: Double  => Double
    case null       => Null
    // blobs
    case b: Array[Byte] if b.size <= 255         => TinyBlob
    case b: Array[Byte] if b.size <= 65535       => Blob
    case b: Array[Byte] if b.size <= 16777215    => MediumBlob
    // Date and Time
    case t: java.sql.Timestamp => Timestamp
    case d: java.sql.Date => Date
    case d: java.util.Date => DateTime
    case RawValue(typ, _, _, _) => typ
    case StringValue(_) => VarChar
    case ByteValue(_) => Tiny
    case ShortValue(_) => Short
    case IntValue(_) => Long
    case LongValue(_) => LongLong
    case FloatValue(_) => Float
    case DoubleValue(_) => Double
    case NullValue => Null
    case _ => -1
  }
}
