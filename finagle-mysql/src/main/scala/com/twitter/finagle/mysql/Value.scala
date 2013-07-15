package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.protocol.{BufferReader, BufferWriter, SQLZeroDate, SQLZeroTimestamp, Type}
import java.sql.{Timestamp, Date => SQLDate}
import java.util.Calendar
import java.nio.charset.{Charset => JCharset}

/**
 * Defines a Value ADT that represents values
 * returned from MySQL.
 */
sealed trait Value
case class StringValue(s: String) extends Value
case class BooleanValue(b: Boolean) extends Value
case class ByteValue(b: Byte) extends Value
case class ShortValue(s: Short) extends Value
case class IntValue(i: Int) extends Value
case class LongValue(l: Long) extends Value
case class FloatValue(f: Float) extends Value
case class DoubleValue(d: Double) extends Value
case class TimestampValue(t: Timestamp) extends Value
case class DateValue(d: SQLDate) extends Value

/**
 * Raw values are returned when the value is not supported, that is,
 * the decode method for the type is not implemented.
 */
case class RawStringValue(s: String) extends Value
case class RawBinaryValue(bytes: Array[Byte]) extends Value

/**
 * "" -> EmptyValue
 * SQL NULL -> NullValue
 */
case object EmptyValue extends Value
case object NullValue extends Value

object Value {
  /**
   * Creates a Value given a MySQL type code
   * and a value string. If the mapping is unknown
   * a RawStringValue is returned.
   */
  def apply(typeCode: Int, value: String) =
    if (value == null)
      NullValue
    else if (value.isEmpty)
      EmptyValue
    else
      typeCode match {
        case Type.STRING     => StringValue(value)
        case Type.VAR_STRING => StringValue(value)
        case Type.VARCHAR    => StringValue(value)
        case Type.TINY       => ByteValue(value.toByte)
        case Type.SHORT      => ShortValue(value.toShort)
        case Type.INT24      => IntValue(value.toInt)
        case Type.LONG       => IntValue(value.toInt)
        case Type.LONGLONG   => LongValue(value.toLong)
        case Type.FLOAT      => FloatValue(value.toFloat)
        case Type.DOUBLE     => DoubleValue(value.toDouble)
        case Type.TIMESTAMP  => TimestampValue(value)
        case Type.DATETIME   => TimestampValue(value)
        case Type.DATE       => DateValue(value)
        case _               => RawStringValue(value)
      }

  /**
   * Creates a Value given a MySQL type code
   * and a byte buffer. If the mapping is unknwon
   * a RawBinaryValue is returned.
   */
  def apply(typeCode: Int, buffer: BufferReader, charset: JCharset) = typeCode match {
    case Type.STRING      => StringValue(buffer.readLengthCodedString(charset))
    case Type.VAR_STRING  => StringValue(buffer.readLengthCodedString(charset))
    case Type.VARCHAR     => StringValue(buffer.readLengthCodedString(charset))
    case Type.TINY        => ByteValue(buffer.readByte())
    case Type.SHORT       => ShortValue(buffer.readShort())
    case Type.INT24       => IntValue(buffer.readInt24())
    case Type.LONG        => IntValue(buffer.readInt())
    case Type.LONGLONG    => LongValue(buffer.readLong())
    case Type.FLOAT       => FloatValue(buffer.readFloat())
    case Type.DOUBLE      => DoubleValue(buffer.readDouble())
    case Type.TIMESTAMP   => TimestampValue(buffer.readLengthCodedBytes())
    case Type.DATETIME    => TimestampValue(buffer.readLengthCodedBytes())
    case Type.DATE        => DateValue(buffer.readLengthCodedBytes())

    // TO DO: Verify that Type.{ENUM, SET, NEWDATE} can be read as
    // a length coded set of bytes.
    case _ => RawBinaryValue(buffer.readLengthCodedBytes())
  }
}


object TimestampValue {
  /**
   * Creates a Value from a MySQL String
   * that represents a Timestamp. Falls back
   * onto java.sql.Timestamp to do the string
   * parsing.
   * @param A MySQL formatted TIMESTAMP string YYYY-MM-DD HH:MM:SS.
   */
  def apply(s: String): Value = {
    if (s == null)
      NullValue
    else if (s.isEmpty)
      EmptyValue
    else if (s == SQLZeroTimestamp.toString)
      TimestampValue(SQLZeroTimestamp)
    else
      TimestampValue(Timestamp.valueOf(s))
  }

  /**
   * Creates a TimestampValue from its
   * MySQL binary representation.
   * @param An array of bytes representing a TIMESTAMP written in the
   * MySQL binary protocol.
   */
   def apply(bytes: Array[Byte]): Value = {
    if (bytes.size == 0) {
      return TimestampValue(SQLZeroTimestamp)
    }

    var year, month, day, hour, min, sec, nano = 0
    val br = BufferReader(bytes)

    // If the len was not zero, we can strictly
    // expect year, month, and day to be included.
    if (br.readable(4)) {
      year = br.readUnsignedShort()
      month = br.readUnsignedByte()
      day = br.readUnsignedByte()
    } else {
      // Instead of throwing an exception, return
      // a RawBinaryValue and allow the user to handle this.
      return RawBinaryValue(bytes)
    }


    // if the time-part is 00:00:00, it isn't included.
    if (br.readable(3)) {
      hour = br.readUnsignedByte()
      min = br.readUnsignedByte()
      sec = br.readUnsignedByte()
    }

    // if the sub-seconds are 0, they aren't included.
    if (br.readable(4)) {
      nano = br.readInt()
    }

    val cal = Calendar.getInstance
    cal.set(year, month-1, day, hour, min, sec)

    val ts = new Timestamp(0)
    ts.setTime(cal.getTimeInMillis)
    ts.setNanos(nano)
    TimestampValue(ts)
   }

  /**
   * Write a java.sql.Timestamp into its
   * MySQL binary representation.
   * @param Timestamp to write
   * @param BufferWriter to write to.
   */
  def write(ts: Timestamp, buffer: BufferWriter) = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    buffer.writeByte(11)
    buffer.writeShort(cal.get(Calendar.YEAR))
    buffer.writeByte(cal.get(Calendar.MONTH) + 1) // increment 0 indexed month
    buffer.writeByte(cal.get(Calendar.DATE))
    buffer.writeByte(cal.get(Calendar.HOUR_OF_DAY))
    buffer.writeByte(cal.get(Calendar.MINUTE))
    buffer.writeByte(cal.get(Calendar.SECOND))
    buffer.writeInt(ts.getNanos)
    buffer
  }
}

object DateValue {
  /**
   * Creates a Value from a MySQL String
   * that represents a Date. Falls back
   * onto java.sql.Date to do the string
   * parsing.
   * @param A MySQL formatted TIMESTAMP string YYYY-MM-DD.
   */
  def apply(s: String): Value = {
    if (s == null)
      NullValue
    else if (s.isEmpty)
      EmptyValue
    else if (s == SQLZeroDate.toString)
      DateValue(SQLZeroDate)
    else
      DateValue(SQLDate.valueOf(s))
  }

  /**
   * Creates a DateValue from its
   * MySQL binary representation.
   * @param An array of bytes representing a DATE written in the
   * MySQL binary protocol.
   */
  def apply(bytes: Array[Byte]): Value = {
    if(bytes.size == 0) {
      return DateValue(SQLZeroDate)
    }

    var year, month, day = 0
    val br = BufferReader(bytes)

    if (br.readable(4)) {
      year = br.readUnsignedShort()
      month = br.readUnsignedByte()
      day = br.readUnsignedByte()
    } else {
      return RawBinaryValue(bytes)
    }

    val cal = Calendar.getInstance
    cal.set(year, month-1, day)

    DateValue(new SQLDate(cal.getTimeInMillis))
  }

  /**
   * Writes a java.sql.Date into its
   * MySQL binary representation.
   * @param Date to write
   * @param BufferWriter to write to.
   */
  def write(date: SQLDate, buffer: BufferWriter) = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(date.getTime)
    buffer.writeByte(4)
    buffer.writeShort(cal.get(Calendar.YEAR))
    buffer.writeByte(cal.get(Calendar.MONTH) + 1) // increment 0 indexed month
    buffer.writeByte(cal.get(Calendar.DATE))
    buffer
  }
}
