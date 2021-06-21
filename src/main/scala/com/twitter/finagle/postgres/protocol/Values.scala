package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import java.sql.Timestamp

object Type {
  val BOOL = 16
  val BYTE_A = 17
  val CHAR = 18
  val NAME = 19
  val INT_8 = 20
  val INT_2 = 21
  val INT_4 = 23
  val REG_PROC = 24
  val TEXT = 25
  val OID = 26
  val TID = 27
  val XID = 28
  val CID = 29
  val XML = 142
  val POINT = 600
  val L_SEG = 601
  val PATH = 602
  val BOX = 603
  val POLYGON = 604
  val LINE = 628
  val CIDR = 650
  val FLOAT_4 = 700
  val FLOAT_8 = 701
  val ABS_TIME = 702
  val REL_TIME = 703
  val T_INTERVAL = 704
  val UNKNOWN = 705
  val CIRCLE = 718
  val MONEY = 790
  val MAC_ADDR = 829
  val INET = 869
  val BP_CHAR = 1042
  val VAR_CHAR = 1043
  val DATE = 1082
  val TIME = 1083
  val TIMESTAMP = 1114
  val TIMESTAMP_TZ = 1184
  val INTERVAL = 1186
  val TIME_TZ = 1266
  val BIT = 1560
  val VAR_BIT = 1562
  val NUMERIC = 1700
  val REF_CURSOR = 1790
  val RECORD = 2249
  val VOID = 2278
  val UUID = 2950
  val ENUM = 16448

}

trait ValueParser {
  def parseBoolean(b: ChannelBuffer): Value[Boolean]

  def parseChar(b: ChannelBuffer): Value[String]

  def parseName(b: ChannelBuffer): Value[String]

  def parseInt8(b: ChannelBuffer): Value[Long]

  def parseInt2(b: ChannelBuffer): Value[Int]

  def parseInt4(b: ChannelBuffer): Value[Int]

  def parseText(b: ChannelBuffer): Value[String]

  // TODO is it string?
  def parseOid(b: ChannelBuffer): Value[String]

  def parseFloat4(b: ChannelBuffer): Value[Float]

  def parseFloat8(b: ChannelBuffer): Value[Double]

  // TODO is it string?
  def parseInet(b: ChannelBuffer): Value[String]

  def parseBpChar(b: ChannelBuffer): Value[String]

  def parseVarChar(b: ChannelBuffer): Value[String]

  def parseTimestamp(b: ChannelBuffer): Value[Timestamp]

  def parseTimestampTZ(b: ChannelBuffer): Value[Timestamp]

  def parseEnum(b: ChannelBuffer): Value[String]

}

object StringValueParser extends ValueParser {
  def parseBoolean(b: ChannelBuffer) = Value[Boolean](b.toString(Charsets.Utf8) == "t")

  def parseChar(b: ChannelBuffer) = parseStr(b)

  def parseName(b: ChannelBuffer) = parseStr(b)

  def parseInt8(b: ChannelBuffer) = Value[Long](b.toString(Charsets.Utf8).toLong)

  def parseInt2(b: ChannelBuffer) = parseInt(b)

  def parseInt4(b: ChannelBuffer) = parseInt(b)

  def parseText(b: ChannelBuffer) = parseStr(b)

  def parseOid(b: ChannelBuffer) = parseStr(b)

  def parseFloat4(b: ChannelBuffer) = Value[Float](b.toString(Charsets.Utf8).toFloat)

  def parseFloat8(b: ChannelBuffer) = Value[Double](b.toString(Charsets.Utf8).toDouble)

  def parseInet(b: ChannelBuffer) = parseStr(b)

  def parseBpChar(b: ChannelBuffer) = parseStr(b)

  def parseVarChar(b: ChannelBuffer) = parseStr(b)

  def parseTimestamp(b: ChannelBuffer) = Value[Timestamp](Timestamp.valueOf(b.toString(Charsets.Utf8)))

  def parseTimestampTZ(b: ChannelBuffer) = parseTimestamp(b)

  def parseEnum(b: ChannelBuffer) = parseStr(b)

  private[this] def parseInt(b: ChannelBuffer) = Value[Int](b.toString(Charsets.Utf8).toInt)

  private[this] def parseStr(b: ChannelBuffer) = Value[String](b.toString(Charsets.Utf8))

}

object ValueParser {

  def parserOf(format: Int, dataType: Int): ChannelBuffer => Value[Any] = {
    val valueParser: ValueParser = format match {
      case 0 => StringValueParser
      case _ => throw new UnsupportedOperationException("TODO Add support for binary format")
    }

    import Type._
    val r: ChannelBuffer => Value[Any] =
      dataType match {
        case BOOL => valueParser.parseBoolean
        case CHAR => valueParser.parseChar
        case NAME => valueParser.parseName
        case INT_8 => valueParser.parseInt8
        case INT_2 => valueParser.parseInt2
        case INT_4 => valueParser.parseInt4
        case TEXT => valueParser.parseText
        case OID => valueParser.parseOid
        case FLOAT_4 => valueParser.parseFloat4
        case FLOAT_8 => valueParser.parseFloat8
        case INET => valueParser.parseInet
        case BP_CHAR => valueParser.parseBpChar
        case VAR_CHAR => valueParser.parseVarChar
        case TIMESTAMP => valueParser.parseTimestamp
        case TIMESTAMP_TZ => valueParser.parseTimestampTZ
        case ENUM => valueParser.parseEnum
        case _ => throw Errors.client("Data type '" + dataType + "' is not supported")
      }
    r

  }
}

object StringValueEncoder {
  def encode(value: Any): ChannelBuffer = {
    value match {
      case s: String => encodeString(s)
      case t: Timestamp => encodeTimestamp(t)
      case _ => encodeError()
    }
  }

  private[this] def encodeString(s: String): ChannelBuffer = {
    val result = ChannelBuffers.dynamicBuffer()
    result.writeBytes(s.getBytes(Charsets.Utf8))
    result
  }

  private[this] def encodeTimestamp(t: Timestamp): ChannelBuffer = {
    encodeString(t.toString())
  }

  private[this] def encodeError(): ChannelBuffer = {
    val buf = ChannelBuffers.dynamicBuffer()
    buf.writeInt(-1)
    buf
  }
}