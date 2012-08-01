package com.twitter.finagle.mysql.protocol

import java.sql.{Timestamp, Date => SQLDate}
import java.util.{Date, Calendar}

/**
 * Collection of methods that converts complex (non-primitive) types
 * into their MySQL binary representation.
 */
object Types {
  /**
   * Write a java.sql.Timestamp into its
   * MySQL binary representation.
   * @param ts Timestamp to write
   * @param buffer BufferWriter that handles writing.
   */
  def writeTimestamp(ts: Timestamp, buffer: BufferWriter) = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    buffer.writeByte(11)
    buffer.writeShort(cal.get(Calendar.YEAR))
    buffer.writeByte(cal.get(Calendar.MONTH))
    buffer.writeByte(cal.get(Calendar.DATE))
    buffer.writeByte(cal.get(Calendar.HOUR_OF_DAY))
    buffer.writeByte(cal.get(Calendar.MINUTE))
    buffer.writeByte(cal.get(Calendar.SECOND))
    buffer.writeInt(ts.getNanos)
    buffer
  }

  def writeDate(d: Date, b: BufferWriter) = Types.writeTimestamp(new Timestamp(d.getTime), b)
  def writeSQLDate(d: SQLDate, b: BufferWriter) = Types.writeTimestamp(new Timestamp(d.getTime), b)
}

/**
 * MySQL Type codes.
 */
object TypeCodes {
  val DECIMAL     = 0x00;
  val TINY        = 0x01;
  val SHORT       = 0x02;
  val LONG        = 0x03;
  val FLOAT       = 0x04;
  val DOUBLE      = 0x05;
  val NULL        = 0x06;
  val TIMESTAMP   = 0x07;
  val LONGLONG    = 0x08;
  val INT24       = 0x09;
  val DATE        = 0x0a;
  val TIME        = 0x0b;
  val DATETIME    = 0x0c;
  val YEAR        = 0x0d;
  val NEWDATE     = 0x0e;
  val VARCHAR     = 0x0f;
  val BIT         = 0x10;
  val NEWDECIMAL  = 0xf6;
  val ENUM        = 0xf7;
  val SET         = 0xf8;
  val TINY_BLOB   = 0xf9;
  val MEDIUM_BLOB = 0xfa;
  val LONG_BLOB   = 0xfb;
  val BLOB        = 0xfc;
  val VAR_STRING  = 0xfd;
  val STRING      = 0xfe;
  val GEOMETRY    = 0xff;
}

/**
 * Timestamp object that can appropriately
 * represent MySQL zero Timestamp.
 */
case object SQLZeroTimestamp extends Timestamp(0) {
  override val getTime = 0L
  override val toString = "0000-00-00 00:00:00"
}

/**
 * Date object that can appropriately
 * represent MySQL zero Date.
 */
case object SQLZeroDate extends SQLDate(0) {
  override val getTime = 0L
  override val toString = "0000-00-00"
}

