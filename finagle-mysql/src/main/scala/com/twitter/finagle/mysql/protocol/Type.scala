package com.twitter.finagle.exp.mysql.protocol

import java.nio.charset.{Charset => JCharset}

object Type {
  /** MySQL type codes */
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

  /**
   * Returns the sizeof the given parameter in
   * its MySQL binary representation. If the size
   * is unknown -1 is returned.
   */
  def sizeOf(any: Any, charset: JCharset = Charset.defaultCharset) = any match {
    case s: String => {
      val bytes = s.getBytes(charset)
      Buffer.sizeOfLen(bytes.size) + bytes.size
    }
    case b: Array[Byte] => Buffer.sizeOfLen(b.size) + b.size
    case b: Boolean     => 1
    case b: Byte        => 1
    case s: Short       => 2
    case i: Int         => 4
    case l: Long        => 8
    case f: Float       => 4
    case d: Double      => 8
    case null           => 0
    // Date and Time
    case t: java.sql.Timestamp    => 12
    case d: java.sql.Date         => 5
    case d: java.util.Date        => 12
    case _ => -1
  }

  /**
   * Retrieves the MySQL type code for the
   * given parameter. If the parameter type
   * mapping is unknown -1 is returned.
   */
  def getCode(any: Any) = any match {
    // primitives
    case s: String  => VARCHAR
    case b: Boolean => TINY
    case b: Byte    => TINY
    case s: Short   => SHORT
    case i: Int     => LONG
    case l: Long    => LONGLONG
    case f: Float   => FLOAT
    case d: Double  => DOUBLE
    case null       => NULL
    // blobs
    case b: Array[Byte] if b.size <= 255         => TINY_BLOB
    case b: Array[Byte] if b.size <= 65535       => BLOB
    case b: Array[Byte] if b.size <= 16777215    => MEDIUM_BLOB

    // No support for LONG_BLOBS. In order to implement this correctly
    // in Java/Scala we need to represent this set of bytes as a composition
    // of buffers.
    // case b: Array[Byte] if b.size <= 4294967295L => LONG_BLOB

    // Date and Time
    case t: java.sql.Timestamp => TIMESTAMP
    case d: java.sql.Date => DATE
    case d: java.util.Date => DATETIME
    case _ => -1
  }
}

/**
 * Timestamp object that can appropriately
 * represent MySQL zero Timestamp.
 */
case object SQLZeroTimestamp extends java.sql.Timestamp(0) {
  override val getTime = 0L
  override val toString = "0000-00-00 00:00:00"
}

/**
 * Date object that can appropriately
 * represent MySQL zero Date.
 */
case object SQLZeroDate extends java.sql.Date(0) {
  override val getTime = 0L
  override val toString = "0000-00-00"
}
