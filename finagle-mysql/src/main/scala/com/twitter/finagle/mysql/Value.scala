package com.twitter.finagle.exp.mysql

import java.util.Calendar
import java.sql.{Date, Timestamp, Time}
import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, BufferWriter}

/**
 * Defines a Value ADT that represents the domain of values
 * received from a mysql server.
 */
sealed trait Value
case class ByteValue(b: Byte) extends Value
case class ShortValue(s: Short) extends Value
case class IntValue(i: Int) extends Value
case class LongValue(l: Long) extends Value
case class FloatValue(f: Float) extends Value
case class DoubleValue(d: Double) extends Value
case class StringValue(s: String) extends Value
case object EmptyValue extends Value
case object NullValue extends Value

/**
 * A RawValue contains the raw bytes that represent
 * a value and enough meta data to decode the
 * bytes.
 *
 * @param typ The mysql type code for this value.
 * @param charset The charset encoding of the bytes.
 * @param isBinary Disambiguates between the text and binary protocol.
 * @param bytes The raw bytes for this value.
 */
case class RawValue(
  typ: Short,
  charset: Short,
  isBinary: Boolean,
  bytes: Array[Byte]
) extends Value

object TimestampValue {
  /**
   * Creates a RawValue from a java.sql.Timestamp
   */
  def apply(ts: Timestamp): Value = {
    val bytes = new Array[Byte](11)
    val bw = BufferWriter(bytes)
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    bw.writeShort(cal.get(Calendar.YEAR))
    bw.writeByte(cal.get(Calendar.MONTH) + 1) // increment 0 indexed month
    bw.writeByte(cal.get(Calendar.DATE))
    bw.writeByte(cal.get(Calendar.HOUR_OF_DAY))
    bw.writeByte(cal.get(Calendar.MINUTE))
    bw.writeByte(cal.get(Calendar.SECOND))
    bw.writeInt(ts.getNanos)
    RawValue(Type.Timestamp, Charset.Binary, true, bytes)
  }

  /**
   * Value extractor for java.sql.Timestamp
   */
  def unapply(v: Value): Option[Timestamp] = v match {
    case RawValue(t, Charset.Binary, false, bytes)
      if (t == Type.Timestamp || t == Type.DateTime) =>
        val str = new String(bytes, Charset(Charset.Binary))
        if (str == Zero.toString) Some(Zero)
        else Some(Timestamp.valueOf(str))

    case RawValue(t, Charset.Binary, true, bytes)
      if (t == Type.Timestamp || t == Type.DateTime) =>
        Some(fromBytes(bytes))

    case _ => None
  }

  /**
   * Timestamp object that can appropriately
   * represent MySQL zero Timestamp.
   */
  private[this] object Zero extends Timestamp(0) {
    override val getTime = 0L
    override val toString = "0000-00-00 00:00:00"
  }

  /**
   * Creates a Timestamp from a Mysql binary representation.
   * Invalid DATETIME or TIMESTAMP values are converted to
   * the “zero” value ('0000-00-00 00:00:00').
   * @param An array of bytes representing a TIMESTAMP written in the
   * MySQL binary protocol.
   */
  private[this] def fromBytes(bytes: Array[Byte]): Timestamp = {
    if (bytes.isEmpty) {
      return Zero
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
      return Zero
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
    ts
   }
}

object DateValue {
  /**
   * Creates a RawValue from a java.sql.Date
   */
  def apply(date: Date): Value = {
    val bytes = new Array[Byte](4)
    val bw = BufferWriter(bytes)
    val cal = Calendar.getInstance
    cal.setTimeInMillis(date.getTime)
    bw.writeShort(cal.get(Calendar.YEAR))
    bw.writeByte(cal.get(Calendar.MONTH) + 1) // increment 0 indexed month
    bw.writeByte(cal.get(Calendar.DATE))
    RawValue(Type.Date, Charset.Binary, true, bytes)
  }

  /**
   * Value extractor for java.sql.Date
   */
  def unapply(v: Value): Option[Date] = v match {
    case RawValue(Type.Date, Charset.Binary, false, bytes) =>
      val str = new String(bytes, Charset(Charset.Binary))
      if (str == Zero.toString) Some(Zero)
      else Some(Date.valueOf(str))

    case RawValue(Type.Date, Charset.Binary, true, bytes) =>
      Some(fromBytes(bytes))
    case _ => None
  }

  /**
   * Date object that can appropriately
   * represent MySQL zero Date.
   */
  private[this] object Zero extends Date(0) {
    override val getTime = 0L
    override val toString = "0000-00-00"
  }

  /**
   * Creates a DateValue from its MySQL binary representation.
   * Invalid DATE values are converted to
   * the “zero” value of the appropriate type
   * ('0000-00-00' or '0000-00-00 00:00:00').
   * @param An array of bytes representing a DATE written in the
   * MySQL binary protocol.
   */
  private[this] def fromBytes(bytes: Array[Byte]): Date = {
    if(bytes.isEmpty) {
      return Zero
    }

    var year, month, day = 0
    val br = BufferReader(bytes)

    if (br.readable(4)) {
      year = br.readUnsignedShort()
      month = br.readUnsignedByte()
      day = br.readUnsignedByte()
    } else {
      return Zero
    }

    val cal = Calendar.getInstance
    cal.set(year, month-1, day)

    new Date(cal.getTimeInMillis)
  }
}

object BigDecimalValue {
  def apply(b: BigDecimal): Value = {
    val str = b.toString.getBytes(Charset(Charset.Binary))
    RawValue(Type.NewDecimal, Charset.Binary, true, str)
  }

  def unapply(v: Value): Option[BigDecimal] = v match {
    case RawValue(Type.NewDecimal, Charset.Binary, _, bytes) =>
      Some(BigDecimal(new String(bytes, Charset(Charset.Binary))))
    case _ => None
  }
}