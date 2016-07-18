package com.twitter.finagle.postgres.values

import java.math.BigInteger
import java.net.InetAddress
import java.nio.charset.{Charset, StandardCharsets}

import com.twitter.logging.Logger
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time._
import java.time.temporal.{ChronoField, JulianFields}
import java.util.{Date, TimeZone, UUID}

import com.twitter.util.{Return, Try}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

/*
 * Simple wrapper around a value in a Postgres row.
 */
case class Value[+A](value: A)

/*
 * Enumeration of value types that can be included in query results.
 */
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
}

trait ValueDecoder[+T] {
  def decodeText(text: String): Try[Value[T]]
  def decodeBinary(bytes: ChannelBuffer, charset: Charset): Try[Value[T]]
}

object ValueDecoder {

  def instance[T](text: String => Try[T], binary: (ChannelBuffer, Charset) => Try[T]): ValueDecoder[T] = new ValueDecoder[T] {
    def decodeText(s: String) = text(s).map(t => Value(t))
    def decodeBinary(b: ChannelBuffer, charset: Charset) = binary(b, charset).map(t => Value(t))
  }

  private def readInetAddress(buf: ChannelBuffer) = {
    val family = buf.readByte()
    val bits = buf.readByte()
    val i = buf.readByte()
    val nb = buf.readByte()
    val arr = Array.fill(nb)(buf.readByte())
    InetAddress.getByAddress(arr)
  }

  val Boolean = instance(s => Return(s == "t" || s == "true"), (b,c) => Try(b.readByte() != 0))
  val Bytea = instance(
    s => Try(s.stripPrefix("\\x").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)),
    (b,c) => Try(Buffers.readBytes(b)))
  val String = instance(s => Return(s), (b,c) => Try(Buffers.readString(b, c)))
  val Int2 = instance(s => Try(s.toShort), (b,c) => Try(b.readShort()))
  val Int4 = instance(s => Try(s.toInt), (b,c) => Try(b.readInt()))
  val Int8 = instance(s => Try(s.toLong), (b,c) => Try(b.readLong()))
  val Float4 = instance(s => Try(s.toFloat), (b,c) => Try(b.readFloat()))
  val Float8 = instance(s => Try(s.toDouble), (b,c) => Try(b.readDouble()))
  val Oid = instance(s => Try(s.toLong), (b,c) => Try(Integer.toUnsignedLong(b.readInt())))
  val Inet = instance(s => Try(InetAddress.getByName(s)), (b, c) => Try(readInetAddress(b)))
  val Date = instance(
    s => Try(LocalDate.parse(s)),
    (b, c) => Try(LocalDate.now().`with`(JulianFields.JULIAN_DAY, b.readInt() + 2451545)))
  val Time = instance(
    s => Try(LocalTime.parse(s)),
    (b, c) => Try(LocalTime.ofNanoOfDay(b.readLong() * 1000))
  )
  val TimeTz = instance(
    s => Try(DateTimeUtils.parseTimeTz(s)),
    (b, c) => Try(DateTimeUtils.readTimeTz(b))
  )
  val Timestamp = instance(
    s => Try(LocalDateTime.ofInstant(java.sql.Timestamp.valueOf(s).toInstant, ZoneId.systemDefault())),
    (b, c) => Try(LocalDateTime.ofInstant(DateTimeUtils.readTimestamp(b), ZoneOffset.UTC))
  )
  val TimestampTZ = instance(
    s => Try {
      val (str, zoneOffs) = DateTimeUtils.ZONE_REGEX.findFirstMatchIn(s) match {
        case Some(m) => m.group(1) -> (m.group(2) match {
          case "-" => -1 * m.group(3).toInt
          case "+" => m.group(3).toInt
        })
        case None => throw new DateTimeException("TimestampTZ string could not be parsed")
      }
      val zone = ZoneId.ofOffset("", ZoneOffset.ofHours(zoneOffs))
      LocalDateTime.ofInstant(
        java.sql.Timestamp.valueOf(str).toInstant,
        zone).atZone(zone)
    },
    (b, c) => Try(DateTimeUtils.readTimestamp(b).atZone(ZoneId.systemDefault()))
  )
  val Interval = instance(
    s => Try(com.twitter.finagle.postgres.values.Interval.parse(s)),
    (b, c) => Try(DateTimeUtils.readInterval(b))
  )
  val Uuid = instance(s => Try(UUID.fromString(s)), (b, c) => Try(new UUID(b.readLong(), b.readLong())))
  val Numeric = instance(
    s => Try(BigDecimal(s)),
    (b, c) => Try(Numerics.readNumeric(b))
  )
  val Jsonb = instance(
    s => Return(s),
    (b, c) => Try {
      b.readByte()  //discard version number
      new String(Array.fill(b.readableBytes())(b.readByte()), c)
    }
  )

  val HStore = instance(
    s => Try {
      HStores.parseHStoreString(s)
        .getOrElse(throw new IllegalArgumentException("Invalid format for hstore"))
    },
    (buf, charset) => Try(HStores.decodeHStoreBinary(buf, charset))
  )

  val Unknown = instance[Either[String, Array[Byte]]](
    s => Return(Left(s)),
    (b, c) => Return(Right(Buffers.readBytes(b)))
  )

  def unknownBinary(t: (ChannelBuffer, Charset)): Try[Value[Any]] = Return(Value(Buffers.readBytes(t._1)))

  val decoders: PartialFunction[String, ValueDecoder[T forSome { type T }]] = {
    case "boolrecv" => Boolean
    case "bytearecv" => Bytea
    case   "charrecv" | "namerecv" | "varcharrecv" | "xml_recv" | "json_recv"
         | "textrecv" | "bpcharrecv" | "cstring_recv" | "citextrecv" | "enum_recv"  => String
    case "int8recv" => Int8
    case "int4recv" => Int4
    case "int2recv" => Int2
    //TODO: cidr
    case "float4recv" => Float4
    case "float8recv" => Float8
    case "inet_recv" => Inet
    case "date_recv" => Date
    case "time_recv" => Time
    case "timetz_recv" => TimeTz
    case "timestamp_recv" => Timestamp
    case "timestamptz_recv" => TimestampTZ
    case "interval_recv" => Interval
    //TODO: bit
    //TODO: varbit
    case "numeric_recv" => Numeric
    case "uuid_recv" => Uuid
    case "jsonb_recv" => Jsonb
    case "hstore_recv" => HStore
  }

}

/**
  * Responsible for encoding a parameter of type T for sending to postgres
  * @tparam T
  */
trait ValueEncoder[-T] {
  def encodeText(t: T): Option[String]
  def encodeBinary(t: T, charset: Charset): Option[ChannelBuffer]
  def recvFunction: String
  def elemRecvFunction: Option[String]

  def contraMap[U](fn: U => T): ValueEncoder[U] = {
    val prev = this
    new ValueEncoder[U] {
      def encodeText(u: U) = prev.encodeText(fn(u))
      def encodeBinary(u: U, charset: Charset) = prev.encodeBinary(fn(u), charset)
      val recvFunction = prev.recvFunction
      val elemRecvFunction = prev.elemRecvFunction
    }
  }
}

object ValueEncoder extends LowPriorityEncoder {

  private val nullParam = {
    val buf = ChannelBuffers.buffer(4)
    buf.writeInt(-1)
    buf
  }

  def instance[T](
    recv: String,
    text: T => String,
    binary: (T, Charset) => Option[ChannelBuffer]
  ): ValueEncoder[T] = new ValueEncoder[T] {
    def encodeText(t: T) = Option(t).map(text)
    def encodeBinary(t: T, c: Charset) = binary(t, c)
    val recvFunction = recv
    val elemRecvFunction = None
  }

  def encodeText[T](t: T, encoder: ValueEncoder[T], charset: Charset = StandardCharsets.UTF_8) =
    Option(t).flatMap(encoder.encodeText) match {
      case None => nullParam
      case Some(str) =>
        val bytes = str.getBytes(charset)
        val buf = ChannelBuffers.buffer(bytes.length + 4)
        buf.writeInt(bytes.length)
        buf.writeBytes(bytes)
        buf
    }

  def encodeBinary[T](t: T, encoder: ValueEncoder[T], charset: Charset = StandardCharsets.UTF_8) =
    Option(t).flatMap(encoder.encodeBinary(_, charset)) match {
      case None => nullParam
      case Some(inBuf) =>
        inBuf.resetReaderIndex()
        val outBuf = ChannelBuffers.buffer(inBuf.readableBytes() + 4)
        outBuf.writeInt(inBuf.readableBytes())
        outBuf.writeBytes(inBuf)
        outBuf
    }

  private def buffer(capacity: Int)(fn: ChannelBuffer => Unit) = {
    val cb = ChannelBuffers.buffer(capacity)
    fn(cb)
    cb
  }

  implicit val string = instance[String](
    "textrecv",
    identity,
    (s, c) => Option(s).map(s => ChannelBuffers.wrappedBuffer(s.getBytes(c)))
  )

  implicit val boolean: ValueEncoder[Boolean] = instance[Boolean](
    "boolrecv",
    b => if(b) "t" else "f",
    (b, c) => Some {
      val buf = ChannelBuffers.buffer(1)
      buf.writeByte(if(b) 1.toByte else 0.toByte)
      buf
    }
  )

  implicit val bytea = instance[Array[Byte]](
    "bytearecv",
    bytes => "\\x" + bytes.map(Integer.toHexString(_)).mkString,
    (b, c) => Some(ChannelBuffers.copiedBuffer(b))
  )
  implicit val int2 = instance[Short]("int2recv", _.toString, (i, c) => Some(buffer(2)(_.writeShort(i))))
  implicit val int4 = instance[Int]("int4recv", _.toString, (i, c) => Some(buffer(4)(_.writeInt(i))))
  implicit val int8 = instance[Long]("int8recv", _.toString, (i, c) => Some(buffer(8)(_.writeLong(i))))
  implicit val float4 = instance[Float]("float4recv", _.toString, (i, c) => Some(buffer(4)(_.writeFloat(i))))
  implicit val float8 = instance[Double]("float8recv", _.toString, (i, c) => Some(buffer(8)(_.writeDouble(i))))
  implicit val date = instance[LocalDate]("date_recv", _.toString, (i, c) =>
    Option(i).map(i => buffer(4)(_.writeInt((i.getLong(JulianFields.JULIAN_DAY) - 2451545).toInt)))
  )
  implicit val timestamp = instance[LocalDateTime](
    "timestamp_recv",
    t => java.sql.Timestamp.valueOf(t).toString,
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestamp(ts))
  )
  implicit val timestampTz = instance[ZonedDateTime](
    "timestamptz_recv",
    t => t.toOffsetDateTime.toString,
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestampTz(ts))
  )
  implicit val time = instance[LocalTime](
    "time_recv",
    t => t.toString,
    (t, c) => Option(t).map(t => buffer(8)(_.writeLong(t.toNanoOfDay / 1000)))
  )
  implicit val timeTz = instance[OffsetTime](
    "timetz_recv",
    t => t.toString,
    (t, c) => Option(t).map(DateTimeUtils.writeTimeTz)
  )
  implicit val interval = instance[Interval](
    "interval_recv",
    i => i.toString,
    (i, c) => Option(i).map(DateTimeUtils.writeInterval)
  )
  implicit val numeric = instance[BigDecimal](
    "numeric_recv",
    d => d.bigDecimal.toPlainString,
    (d, c) => Option(d).map(d => Numerics.writeNumeric(d))
  )
  implicit val uuid = instance[UUID](
    "uuid_recv",
    u => u.toString,
    (u, c) => Option(u).map(u => buffer(16) {
      b =>
        b.writeLong(u.getMostSignificantBits)
        b.writeLong(u.getLeastSignificantBits)
    })
  )
  implicit val hstore = instance[Map[String, Option[String]]](
    "hstore_recv",
    m => HStores.formatHStoreString(m),
    (m, c) => Option(m).map(HStores.encodeHStoreBinary(_, c))
  )
  implicit val hstoreNoNulls = hstore.contraMap {
    m: Map[String, String] => m.mapValues(Option(_))
  }

  implicit def option[T](implicit encodeT: ValueEncoder[T]): ValueEncoder[Option[T]] = new ValueEncoder[Option[T]] {
    val recvFunction = encodeT.recvFunction
    val elemRecvFunction = None
    def encodeText(optT: Option[T]) = optT.flatMap(encodeT.encodeText)
    def encodeBinary(tOpt: Option[T], c: Charset) = tOpt.flatMap(t => encodeT.encodeBinary(t, c))
  }
}

trait LowPriorityEncoder {

}