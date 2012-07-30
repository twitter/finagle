package com.twitter.finagle.mysql.protocol

import java.sql.{Timestamp, Date}

/**
 * Timestamp object that can appropriately
 * represent MySQL zero Timestamp.
 */
case object ZeroTimestamp extends Timestamp(0) {
  override val getTime = 0L
  override val toString = "0000-00-00 00:00:00"
}

/**
 * Date object that can appropriately
 * represent MySQL zero Date.
 */
case object ZeroDate extends Date(0) {
  override val getTime = 0L
  override val toString = "0000-00-00"
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
  val NEWDATE     = 0x0e; // missing
  val VARCHAR     = 0x0f;
  val BIT         = 0x10;
  val NEWDECIMAL  = 0xf6;
  val ENUM        = 0xf7; // missing
  val SET         = 0xf8; // missing
  val TINY_BLOB   = 0xf9;
  val MEDIUM_BLOB = 0xfa;
  val LONG_BLOB   = 0xfb;
  val BLOB        = 0xfc;
  val VAR_STRING  = 0xfd;
  val STRING      = 0xfe;
  val GEOMETRY    = 0xff;
}