package com.twitter.finagle.mysql.protocol

import java.sql.{Timestamp, Date}
import java.util.Calendar

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
case class DateValue(d: Date) extends Value
case class RawStringValue(s: String) extends Value
case class RawBinaryValue(bytes: Array[Byte]) extends Value

case object EmptyValue extends Value
case object NullValue extends Value


object TimestampValue {
  /**
   * Creates a Value from a MySQL String
   * that represents a Timestamp. Falls back
   * onto java.sql.Timestamp to do the string
   * parsing.
   * @param s A MySQL formatted TIMESTAMP string YYYY-MM-DD HH:MM:SS.
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
   * Creates a Timestamp Value from its
   * MySQL binary representation.
   * @param bytes An array of bytes representing a TIMESTAMP written in the
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
}

object DateValue {
  /**
   * Creates a Value from a MySQL String
   * that represents a Date. Falls back
   * onto java.sql.Date to do the string
   * parsing.
   * @param s A MySQL formatted TIMESTAMP string YYYY-MM-DD.
   */
  def apply(s: String): Value = {
    if (s == null) 
      NullValue
    else if (s.isEmpty)
      EmptyValue
    else if (s == SQLZeroDate.toString)
      DateValue(SQLZeroDate)
    else
      DateValue(Date.valueOf(s))
  }

  // TO DO: Parse binary representation of Date.
  def apply(bytes: Array[Byte]): Value = RawBinaryValue(bytes)
}