package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffer

trait ValueParser {
  def parseBoolean(b: ChannelBuffer): BooleanValue

  def parseChar(b: ChannelBuffer): StringValue

  def parseName(b: ChannelBuffer): StringValue

  def parseInt8(b: ChannelBuffer): LongValue

  def parseInt2(b: ChannelBuffer): IntValue

  def parseInt4(b: ChannelBuffer): IntValue

  def parseText(b: ChannelBuffer): StringValue

  // TODO is it string?
  def parseOid(b: ChannelBuffer): StringValue

  def parseFloat4(b: ChannelBuffer): FloatValue

  def parseFloat8(b: ChannelBuffer): DoubleValue

  // TODO is it string?
  def parseInet(b: ChannelBuffer): StringValue

  def parseBpChar(b: ChannelBuffer): StringValue

  def parseVarChar(b: ChannelBuffer): StringValue

  def parseTimestampTZ(b: ChannelBuffer): StringValue

}

object StringValueParser extends ValueParser {
  def parseBoolean(b: ChannelBuffer) = BooleanValue(b.toString(Charsets.Utf8) == "t")

  def parseChar(b: ChannelBuffer) = parseStr(b)

  def parseName(b: ChannelBuffer) = parseStr(b)

  def parseInt8(b: ChannelBuffer) = LongValue(b.toString(Charsets.Utf8).toLong)

  def parseInt2(b: ChannelBuffer) = parseInt(b)

  def parseInt4(b: ChannelBuffer) = parseInt(b)

  def parseText(b: ChannelBuffer) = parseStr(b)

  def parseOid(b: ChannelBuffer) = parseStr(b)

  def parseFloat4(b: ChannelBuffer) = FloatValue(b.toString(Charsets.Utf8).toFloat)

  def parseFloat8(b: ChannelBuffer) = DoubleValue(b.toString(Charsets.Utf8).toDouble)

  def parseInet(b: ChannelBuffer) = parseStr(b)

  def parseBpChar(b: ChannelBuffer) = parseStr(b)

  def parseVarChar(b: ChannelBuffer) = parseStr(b)

  def parseTimestampTZ(b: ChannelBuffer) = parseStr(b)

  private[this] def parseInt(b: ChannelBuffer) = IntValue(b.toString(Charsets.Utf8).toInt)

  private[this] def parseStr(b: ChannelBuffer) = StringValue(b.toString(Charsets.Utf8))

}

object ValueParser {

  def parserOf(f: FieldDescription): ChannelBuffer => Value = {
    val valueParser: ValueParser = f.fieldFormat match {
      case 0 => StringValueParser
      case _ => throw new UnsupportedOperationException("TODO Add support for binary format")
    }

    val r: ChannelBuffer => Value =
      f.dataType match {
        case 16 => valueParser.parseBoolean
        case 18 => valueParser.parseChar
        case 19 => valueParser.parseName
        case 20 => valueParser.parseInt8
        case 21 => valueParser.parseInt2
        case 23 => valueParser.parseInt4
        case 25 => valueParser.parseText
        case 26 => valueParser.parseOid
        case 700 => valueParser.parseFloat4
        case 701 => valueParser.parseFloat8
        case 869 => valueParser.parseInet
        case 1042 => valueParser.parseBpChar
        case 1043 => valueParser.parseVarChar
        case 1184 => valueParser.parseTimestampTZ
        case _ => throw new UnsupportedOperationException("TODO Add support for data type '" + f.dataType + "'")
      }
    r

  }

}


/*

  Bool        ->   16
  ByteA       ->   17
  Char        ->   18
  Name        ->   19
  Int8        ->   20
  Int2        ->   21
  Int4        ->   23
  RegProc     ->   24
  Text        ->   25
  Oid         ->   26
  Tid         ->   27
  Xid         ->   28
  Cid         ->   29
  Xml         ->  142
  Point       ->  600
  LSeg        ->  601
  Path        ->  602
  Box         ->  603
  Polygon     ->  604
  Line        ->  628
  Cidr        ->  650
  Float4      ->  700
  Float8      ->  701
  AbsTime     ->  702
  RelTime     ->  703
  TInterval   ->  704
  Unknown     ->  705
  Circle      ->  718
  Money       ->  790
  MacAddr     ->  829
  Inet        ->  869
  BpChar      -> 1042
  VarChar     -> 1043
  Date        -> 1082
  Time        -> 1083
  Timestamp   -> 1114
  TimestampTZ -> 1184
  Interval    -> 1186
  TimeTZ      -> 1266
  Bit         -> 1560
  VarBit      -> 1562
  Numeric     -> 1700
  RefCursor   -> 1790
  Record      -> 2249
  Void        -> 2278
  UUID        -> 2950

*/