package com.twitter.finagle.postgres

import java.sql.{ Date => SQLDate }
import com.twitter.util.Future
import com.twitter.finagle.postgres.protocol.PgRequest
import com.twitter.finagle.postgres.protocol.PgResponse
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.postgres.protocol.FieldDescription
import org.jboss.netty.buffer.ChannelBuffer
import java.sql.Timestamp
import com.twitter.finagle.postgres.protocol.Query
import com.twitter.finagle.postgres.protocol.Communication
import com.twitter.finagle.postgres.protocol.MessageSequenceResponse
import com.twitter.finagle.postgres.protocol.Charsets
import com.twitter.finagle.postgres.protocol.RowDescription
import com.twitter.finagle.postgres.protocol.ReadyForQuery
import com.twitter.finagle.postgres.protocol.CommandComplete
import com.twitter.finagle.postgres.protocol.DataRow
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.protocol.PgCodec

sealed trait Value {
}

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

case object NullValue extends Value

class Row(val fields: IndexedSeq[Field], val vals: IndexedSeq[Value]) {

  private[this] val indexMap = fields.map(_.name).zipWithIndex.toMap

  def get(name: String): Option[Value] = indexMap.get(name).map(vals(_))

  def get(index: Int): Value = vals(index)

  def values(): IndexedSeq[Value] = vals

  override def toString(): String = "{ fields='" + fields.toString + "', rows='" + vals.toString + "'"

}

case class Field(name: String)

case class ResultSet(fields: IndexedSeq[Field], rows: Seq[Row])

class Client(factory: ServiceFactory[PgRequest, PgResponse]) {
  private[this] lazy val fService = factory.apply()

  def query(sql: String): Future[ResultSet] = {
    send(Communication.request(new Query(sql))) {
      case MessageSequenceResponse(RowDescription(fields) :: entries) =>
        val fieldNames = fields.map(f => Field(f.name))
        val fieldParsers = fields.map(f => ValueParser.parserOf(f))

        val dataRows = entries.filter(_.isInstanceOf[DataRow]).map(_.asInstanceOf[DataRow])
        val rows = dataRows.map(r => {
          new Row(fieldNames, r.data.zip(fieldParsers).map(pair => pair._2(pair._1)))
        })

        Future(new ResultSet(fieldNames, rows))
    }
  }
  
  def close() {
    factory.close()
  }

  private[this] def send[T](r: PgRequest)(handler: PartialFunction[PgResponse, Future[T]]) =
    fService flatMap { service =>
      service(r) flatMap (handler orElse {
        case _ => Future.exception(new UnsupportedOperationException("TODO Support exceptions correctly"));
      })
    }

}

object Client {

  def apply(host: String, username: String, password: Option[String], database: String): Client = {
    val factory: ServiceFactory[PgRequest, PgResponse] = ClientBuilder()
      .codec(new PgCodec(username, password, database))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()

    new Client(factory)
  }

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