package com.twitter.finagle.postgres.values

import java.net.InetAddress
import java.nio.charset.{Charset, StandardCharsets}
import java.sql.Timestamp
import java.time._
import java.time.temporal.JulianFields
import java.util.UUID

import com.twitter.util.{Return, Throw, Try}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import scala.language.existentials

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
  def decodeText(recv: String, text: String): Try[T]
  def decodeBinary(recv: String, bytes: ChannelBuffer, charset: Charset): Try[T]
  def map[U](fn: T => U): ValueDecoder[U] = new ValueDecoder[U] {
    def decodeText(recv: String, text: String) = ValueDecoder.this.decodeText(recv, text).map(fn)
    def decodeBinary(recv: String, bytes: ChannelBuffer, charset: Charset) =
      ValueDecoder.this.decodeBinary(recv, bytes, charset).map(fn)
  }
}

object ValueDecoder {

  def instance[T](text: String => Try[T], binary: (ChannelBuffer, Charset) => Try[T]): ValueDecoder[T] =
    new ValueDecoder[T] {
      def decodeText(recv: String, s: String) = text(s)
      def decodeBinary(recv: String, b: ChannelBuffer, charset: Charset) = binary(b, charset)
    }

  def instance[T](text: (String, String) => Try[T], binary: (String, ChannelBuffer, Charset) => Try[T]): ValueDecoder[T] =
    new ValueDecoder[T] {
      def decodeText(recv: String, s: String) = text(recv, s)
      def decodeBinary(recv: String, b: ChannelBuffer, charset: Charset) = binary(recv, b, charset)
    }

  private def readInetAddress(buf: ChannelBuffer) = {
    val family = buf.readByte()
    val bits = buf.readByte()
    val i = buf.readByte()
    val nb = buf.readByte()
    val arr = Array.fill(nb)(buf.readByte())
    InetAddress.getByAddress(arr)
  }

  implicit val boolean: ValueDecoder[Boolean] = instance(s => Return(s == "t" || s == "true"), (b,c) => Try(b.readByte() != 0))
  implicit val bytea: ValueDecoder[Array[Byte]] = instance(
    s => Try(s.stripPrefix("\\x").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)),
    (b,c) => Try(Buffers.readBytes(b)))

  // for String, we have a special case of `jsonb` - the user might want it as a String, but if it's jsonb we need
  // to strip off the version byte in the binary case
  implicit val string: ValueDecoder[String] =
    instance((recv, s) => Return(s), (recv, b,c) => recv match {
      case "jsonb_recv" => b.readByte; Try(Buffers.readString(b, c))
      case _ => Try(Buffers.readString(b, c))
    })

  implicit val int2: ValueDecoder[Short] = instance(s => Try(s.toShort), (b,c) => Try(b.readShort()))
  implicit val int4: ValueDecoder[Int] = instance(s => Try(s.toInt), (b,c) => Try(b.readInt()))
  implicit val int8: ValueDecoder[Long] = instance(s => Try(s.toLong), (b,c) => Try(b.readLong()))
  implicit val float4: ValueDecoder[Float] = instance(s => Try(s.toFloat), (b,c) => Try(b.readFloat()))
  implicit val float8: ValueDecoder[Double] = instance(s => Try(s.toDouble), (b,c) => Try(b.readDouble()))
  val Oid = instance(s => Try(s.toLong), (b,c) => Try(Integer.toUnsignedLong(b.readInt())))
  implicit val inet: ValueDecoder[InetAddress] = instance(s => Try(InetAddress.getByName(s)), (b, c) => Try(readInetAddress(b)))
  implicit val localDate: ValueDecoder[LocalDate] = instance(
    s => Try(LocalDate.parse(s)),
    (b, c) => Try(LocalDate.now().`with`(JulianFields.JULIAN_DAY, b.readInt() + 2451545)))
  implicit val localTime: ValueDecoder[LocalTime] = instance(
    s => Try(LocalTime.parse(s)),
    (b, c) => Try(LocalTime.ofNanoOfDay(b.readLong() * 1000))
  )
  implicit val offsetTime: ValueDecoder[OffsetTime] = instance(
    s => Try(DateTimeUtils.parseTimeTz(s)),
    (b, c) => Try(DateTimeUtils.readTimeTz(b))
  )
  implicit val localDateTime: ValueDecoder[LocalDateTime] = instance(
    s => Try(LocalDateTime.ofInstant(java.sql.Timestamp.valueOf(s).toInstant, ZoneId.systemDefault())),
    (b, c) => Try(LocalDateTime.ofInstant(DateTimeUtils.readTimestamp(b), ZoneOffset.UTC))
  )

  implicit val instant: ValueDecoder[Instant] = instance(
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
        zone).atZone(zone).toInstant
    },
    (b, c) => Try(DateTimeUtils.readTimestamp(b))
  )

  implicit val zonedDateTime: ValueDecoder[ZonedDateTime] = instant.map(_.atZone(ZoneId.systemDefault()))
  implicit val offsetDateTime: ValueDecoder[OffsetDateTime] = zonedDateTime.map(_.toOffsetDateTime)

  implicit val interval: ValueDecoder[Interval] = instance(
    s => Try(com.twitter.finagle.postgres.values.Interval.parse(s)),
    (b, c) => Try(DateTimeUtils.readInterval(b))
  )

  implicit val uuid: ValueDecoder[UUID] = instance(s => Try(UUID.fromString(s)), (b, c) => Try(new UUID(b.readLong(), b.readLong())))
  implicit val bigDecimal: ValueDecoder[BigDecimal] = instance(
    s => Try(BigDecimal(s)),
    (b, c) => Try(Numerics.readNumeric(b))
  )

  implicit val javaBigDecimal: ValueDecoder[java.math.BigDecimal] = bigDecimal.map(_.bigDecimal)

  val jsonb = instance(
    s => Return(s),
    (b, c) => Try {
      b.readByte()  //discard version number
      new String(Array.fill(b.readableBytes())(b.readByte()), c)
    }
  )

  implicit val hstoreMap: ValueDecoder[Map[String, Option[String]]] = instance(
    s => Try {
      HStores.parseHStoreString(s)
        .getOrElse(throw new IllegalArgumentException("Invalid format for hstore"))
    },
    (buf, charset) => Try(HStores.decodeHStoreBinary(buf, charset))
  )

  val unknown: ValueDecoder[Either[String, Array[Byte]]] = instance(
    s => Return(Left(s)),
    (b, c) => Return(Right(Buffers.readBytes(b)))
  )

  val never: ValueDecoder[Nothing] = instance(
    (recv, _) => Throw(new NoSuchElementException(s"No decoder available for $recv")),
    (recv, _ , _) => Throw(new NoSuchElementException(s"No decoder available for $recv"))
  )

  def unknownBinary(t: (ChannelBuffer, Charset)): Try[Value[Any]] = Return(Value(Buffers.readBytes(t._1)))

  val decoders: PartialFunction[String, ValueDecoder[T forSome { type T }]] = {
    case "boolrecv" => boolean
    case "bytearecv" => bytea
    case   "charrecv" | "namerecv" | "varcharrecv" | "xml_recv" | "json_recv"
         | "textrecv" | "bpcharrecv" | "cstring_recv" | "citextrecv" | "enum_recv"  => string
    case "int8recv" => int8
    case "int4recv" => int4
    case "int2recv" => int2
    //TODO: cidr
    case "float4recv" => float4
    case "float8recv" => float8
    case "inet_recv" => inet
    case "date_recv" => localDate
    case "time_recv" => localTime
    case "timetz_recv" => offsetTime
    case "timestamp_recv" => localDateTime
    case "timestamptz_recv" => instant
    case "interval_recv" => interval
    //TODO: bit
    //TODO: varbit
    case "numeric_recv" => bigDecimal
    case "uuid_recv" => uuid
    case "jsonb_recv" => jsonb
    case "hstore_recv" => hstoreMap
  }

}

/**
  * Typeclass responsible for encoding a parameter of type T for sending to postgres
  * @tparam T The type which it encodes
  */
trait ValueEncoder[-T] {
  def encodeText(t: T): Option[String]
  def encodeBinary(t: T, charset: Charset): Option[ChannelBuffer]
  def typeName: String
  def elemTypeName: Option[String]

  def contraMap[U](fn: U => T, newTypeName: String = this.typeName, newElemTypeName: Option[String] = elemTypeName): ValueEncoder[U] = {
    val prev = this
    new ValueEncoder[U] {
      def encodeText(u: U) = prev.encodeText(fn(u))
      def encodeBinary(u: U, charset: Charset) = prev.encodeBinary(fn(u), charset)
      val typeName = newTypeName
      val elemTypeName = newElemTypeName
    }
  }
}

object ValueEncoder extends LowPriorityEncoder {

  case class Exported[T](encoder: ValueEncoder[T])

  private val nullParam = {
    val buf = ChannelBuffers.buffer(4)
    buf.writeInt(-1)
    buf
  }

  def instance[T](
    instanceTypeName: String,
    text: T => String,
    binary: (T, Charset) => Option[ChannelBuffer]
  ): ValueEncoder[T] = new ValueEncoder[T] {
    def encodeText(t: T) = Option(t).map(text)
    def encodeBinary(t: T, c: Charset) = binary(t, c)
    val typeName = instanceTypeName
    val elemTypeName = None
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

  implicit val string: ValueEncoder[String] = instance(
    "text",
    identity,
    (s, c) => Option(s).map(s => ChannelBuffers.wrappedBuffer(s.getBytes(c)))
  )

  implicit val boolean: ValueEncoder[Boolean] = instance(
    "bool",
    b => if(b) "t" else "f",
    (b, c) => Some {
      val buf = ChannelBuffers.buffer(1)
      buf.writeByte(if(b) 1.toByte else 0.toByte)
      buf
    }
  )

  implicit val bytea: ValueEncoder[Array[Byte]] = instance(
    "bytea",
    bytes => "\\x" + bytes.map("%02x".format(_)).mkString,
    (b, c) => Some(ChannelBuffers.copiedBuffer(b))
  )
  implicit val int2: ValueEncoder[Short] = instance("int2", _.toString, (i, c) => Some(buffer(2)(_.writeShort(i))))
  implicit val int4: ValueEncoder[Int] = instance("int4", _.toString, (i, c) => Some(buffer(4)(_.writeInt(i))))
  implicit val int8: ValueEncoder[Long] = instance("int8", _.toString, (i, c) => Some(buffer(8)(_.writeLong(i))))
  implicit val float4: ValueEncoder[Float] = instance("float4", _.toString, (i, c) => Some(buffer(4)(_.writeFloat(i))))
  implicit val float8: ValueEncoder[Double] = instance("float8", _.toString, (i, c) => Some(buffer(8)(_.writeDouble(i))))
  implicit val date: ValueEncoder[LocalDate] = instance("date", _.toString, (i, c) =>
    Option(i).map(i => buffer(4)(_.writeInt((i.getLong(JulianFields.JULIAN_DAY) - 2451545).toInt)))
  )
  implicit val timestamp: ValueEncoder[LocalDateTime] = instance(
    "timestamp",
    t => java.sql.Timestamp.valueOf(t).toString,
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestamp(ts))
  )
  implicit val timestampTz: ValueEncoder[ZonedDateTime] = instance(
    "timestamptz", { t =>
      val offs = t.toOffsetDateTime
      val hours = (offs.getOffset.getTotalSeconds / 3600).formatted("%+03d")
      Timestamp.from(t.toInstant).toString + hours
    },
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestampTz(ts))
  )
  implicit val instant: ValueEncoder[Instant] = instance(
    "timestamptz",
    i => Timestamp.from(i).toString + "+00",
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeInstant(ts))
  )
  implicit val time: ValueEncoder[LocalTime] = instance(
    "time",
    t => t.toString,
    (t, c) => Option(t).map(t => buffer(8)(_.writeLong(t.toNanoOfDay / 1000)))
  )
  implicit val timeTz: ValueEncoder[OffsetTime] = instance(
    "timetz",
    t => t.toString,
    (t, c) => Option(t).map(DateTimeUtils.writeTimeTz)
  )
  implicit val interval: ValueEncoder[Interval] = instance(
    "interval",
    i => i.toString,
    (i, c) => Option(i).map(DateTimeUtils.writeInterval)
  )
  implicit val numeric: ValueEncoder[BigDecimal] = instance(
    "numeric",
    d => d.bigDecimal.toPlainString,
    (d, c) => Option(d).map(d => Numerics.writeNumeric(d))
  )
  implicit val numericJava: ValueEncoder[java.math.BigDecimal] = instance(
    "numeric",
    d => d.toPlainString,
    (d, c) => Option(d).map(d => Numerics.writeNumeric(BigDecimal(d)))
  )
  implicit val numericBigInt: ValueEncoder[BigInt] = instance(
    "numeric",
    i => i.toString,
    (i, c) => Option(i).map(i => Numerics.writeNumeric(BigDecimal(i)))
  )
  implicit val numericJavaBigInt: ValueEncoder[java.math.BigInteger] = instance(
    "numeric",
    i => i.toString,
    (i, c) => Option(i).map(i => Numerics.writeNumeric(BigDecimal(i)))
  )
  implicit val uuid: ValueEncoder[UUID] = instance(
    "uuid",
    u => u.toString,
    (u, c) => Option(u).map(u => buffer(16) {
      b =>
        b.writeLong(u.getMostSignificantBits)
        b.writeLong(u.getLeastSignificantBits)
    })
  )
  implicit val hstore: ValueEncoder[Map[String, Option[String]]] = instance[Map[String, Option[String]]](
    "hstore",
    m => HStores.formatHStoreString(m),
    (m, c) => Option(m).map(HStores.encodeHStoreBinary(_, c))
  )
  implicit val hstoreNoNulls: ValueEncoder[Map[String, String]] = hstore.contraMap {
    m: Map[String, String] => m.mapValues(Option(_))
  }

  implicit def option[T](implicit encodeT: ValueEncoder[T]): ValueEncoder[Option[T]] = new ValueEncoder[Option[T]] {
    val typeName = encodeT.typeName
    val elemTypeName = encodeT.elemTypeName
    def encodeText(optT: Option[T]) = optT.flatMap(encodeT.encodeText)
    def encodeBinary(tOpt: Option[T], c: Charset) = tOpt.flatMap(t => encodeT.encodeBinary(t, c))
  }
}

trait LowPriorityEncoder {
  implicit def fromExport[T](implicit export: ValueEncoder.Exported[T]): ValueEncoder[T] = export.encoder
}