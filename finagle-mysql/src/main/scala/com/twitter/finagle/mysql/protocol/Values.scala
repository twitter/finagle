package com.twitter.finagle.mysql.protocol

import java.sql.{Timestamp, Date}

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
case class DoubleValue(f: Double) extends Value
case class TimestampValue(t: Timestamp) extends Value
case class DateValue(d: Date) extends Value
case class RawValue(str: String) extends Value
case class RawBinaryValue(bytes: Array[Byte]) extends Value

case object EmptyValue extends Value
case object NullValue extends Value

object Value {
  /** 
   * Converts a String into a StringValue if not
   * null or empty.
   */
  def toStringValue(str: String): Value = 
    if (str == null)
      NullValue
    else if (str.isEmpty)
      EmptyValue
    else
      StringValue(str)

  /**
   * Converts a String into a TimestampValue
   * if not null or empty.
   */
  def toTimestampValue(str: String): Value = 
    if (str == null)
      NullValue
    else if (str.isEmpty) 
      EmptyValue 
    else if (str == ZeroTimestamp.toString)
      TimestampValue(ZeroTimestamp)
    else
      TimestampValue(Timestamp.valueOf(str))

  /**
   * Converts a String into a DateValue
   * if not null or empty.
   */
  def toDateValue(str: String): Value =
    if (str == null)
      NullValue
    else if (str.isEmpty)
      EmptyValue
    else if (str == ZeroDate.toString)
      DateValue(ZeroDate)
    else
      DateValue(Date.valueOf(str))
}