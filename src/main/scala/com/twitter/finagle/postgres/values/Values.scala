package com.twitter.finagle.postgres.values

import java.net.InetAddress
import java.nio.charset.Charset

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

object TimeUtils {
  val POSTGRES_EPOCH_MICROS = 946684800000000L
  val ZONE_REGEX = "(.*)(-|\\+)([0-9]{2})".r

  def readTimestamp(buf: ChannelBuffer) = {
    val micros = buf.readLong() + POSTGRES_EPOCH_MICROS
    val seconds = micros / 1000000L
    val nanos = (micros - seconds * 1000000L) * 1000
    Instant.ofEpochSecond(seconds, nanos)
  }

  def writeTimestamp(timestamp: LocalDateTime) = {
    val instant = timestamp.atZone(ZoneId.systemDefault()).toInstant
    val seconds = instant.getEpochSecond
    val micros = instant.getLong(ChronoField.MICRO_OF_SECOND) + seconds * 1000000
    val buf = ChannelBuffers.buffer(8)
    buf.writeLong(micros - POSTGRES_EPOCH_MICROS)
    buf
  }
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

  private def getUnsignedShort(buf: ChannelBuffer) = {
    val high = buf.readByte().toInt
    val low = buf.readByte()
    (high << 8) | low
  }

  private val NUMERIC_POS = 0x0000
  private val NUMERIC_NEG = 0x4000
  private val NUMERIC_NAN = 0xC000
  private val NUMERIC_NULL = 0xF000
  private val NumericDigitBaseExponent = 4

  private def readNumeric(buf: ChannelBuffer) = {
    val len = getUnsignedShort(buf)
    val weight = buf.readShort()
    val sign = getUnsignedShort(buf)
    val displayScale = getUnsignedShort(buf)

    //digits are actually unsigned base-10000 numbers (not straight up bytes)
    val digits = Array.fill(len)(buf.readShort())
    val bdDigits = digits.map(BigDecimal(_))

    if(bdDigits.length > 0) {
      val unscaled = bdDigits.tail.foldLeft(bdDigits.head) {
        case (accum, digit) => BigDecimal(accum.bigDecimal.scaleByPowerOfTen(NumericDigitBaseExponent)) + digit
      }

      val firstDigitSize =
        if (digits.head < 10) 1
        else if (digits.head < 100) 2
        else if (digits.head < 1000) 3
        else 4

      val scaleFactor = if (weight >= 0)
        weight * NumericDigitBaseExponent + firstDigitSize
      else
        weight * NumericDigitBaseExponent + firstDigitSize
      val unsigned = unscaled.bigDecimal.movePointLeft(unscaled.precision).movePointRight(scaleFactor).setScale(displayScale)

      sign match {
        case NUMERIC_POS => BigDecimal(unsigned)
        case NUMERIC_NEG => BigDecimal(unsigned.negate())
        case NUMERIC_NAN => throw new NumberFormatException("Decimal is NaN")
        case NUMERIC_NULL => throw new NumberFormatException("Decimal is NUMERIC_NULL")
      }
    } else {
      BigDecimal(0)
    }
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
  val Timestamp = instance(
    s => Try(LocalDateTime.ofInstant(java.sql.Timestamp.valueOf(s).toInstant, ZoneId.systemDefault())),
    (b, c) => Try(LocalDateTime.ofInstant(TimeUtils.readTimestamp(b), ZoneOffset.UTC))
  )
  val TimestampTZ = instance(
    s => Try {
      val (str, zoneOffs) = TimeUtils.ZONE_REGEX.findFirstMatchIn(s) match {
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
    (b, c) => Try(TimeUtils.readTimestamp(b).atZone(ZoneId.systemDefault()))
  )
  val Uuid = instance(s => Try(UUID.fromString(s)), (b, c) => Try(new UUID(b.readLong(), b.readLong())))
  val Numeric = instance(
    s => Try(BigDecimal(s)),
    (b, c) => Try(readNumeric(b))
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
      HStoreStringParser(s)
        .getOrElse(throw new IllegalArgumentException("Invalid format for hstore"))
        .mapValues(Some.apply)  //TODO: HStoreStringParser should handle NULLs and return Map[String, Option[String]]
    },
    (b, c) => Try {
      val count = b.readInt()
      val pairs = Array.fill(count) {
        val keyLength = b.readInt()
        val key = Array.fill(keyLength)(b.readByte())
        val valueLength = b.readInt()
        val value = valueLength match {
          case -1 => None
          case l =>
            val valueBytes = Array.fill(l)(b.readByte())
            Some(valueBytes)
        }
        new String(key, c) -> value.map(new String(_, c))
      }
      pairs.toMap
    }
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
    case "timestamp_recv" => Timestamp
    case "timestamptz_recv" => TimestampTZ
    //TODO: timetz
    //TODO: interval
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
  def encodeText(t: T): String
  def encodeBinary(t: T, charset: Charset): ChannelBuffer
  def recvFunction: String
  def elemRecvFunction: Option[String]
}

object ValueEncoder {

  def instance[T](recv: String, text: T => String, binary: (T, Charset) => ChannelBuffer): ValueEncoder[T] = new ValueEncoder[T] {
    def encodeText(t: T) = text(t)
    def encodeBinary(t: T, c: Charset) = binary(t, c)
    def recvFunction = recv
    def elemRecvFunction = None
  }

  private def buffer(capacity: Int)(fn: ChannelBuffer => Unit) = {
    val cb = ChannelBuffers.buffer(capacity)
    fn(cb)
    cb
  }

  implicit val String = instance[String]("textrecv", identity, (s, c) => ChannelBuffers.wrappedBuffer(s.getBytes(c)))
  implicit val Boolean = instance[Boolean](
    "boolrecv",
    b => if(b) "t" else "f",
    (b, c) => ChannelBuffers.wrappedBuffer(Array(if(b) 1.toByte else 0.toByte)))
  implicit val Bytea = instance[Array[Byte]](
    "bytearecv",
    bytes => "\\x" + bytes.map(Integer.toHexString(_)).mkString,
    (b, c) => ChannelBuffers.copiedBuffer(b)
  )
  implicit val Int2 = instance[Short]("int2recv", _.toString, (i, c) => buffer(2)(_.writeShort(i)))
  implicit val Int4 = instance[Int]("int4recv", _.toString, (i, c) => buffer(4)(_.writeInt(i)))
  implicit val Int8 = instance[Long]("int8recv", _.toString, (i, c) => buffer(8)(_.writeLong(i)))
  implicit val Float4 = instance[Float]("float4recv", _.toString, (i, c) => buffer(4)(_.writeFloat(i)))
  implicit val Float8 = instance[Double]("float8recv", _.toString, (i, c) => buffer(8)(_.writeDouble(i)))
  implicit val Date = instance[LocalDate]("date_recv", _.toString, (i, c) => buffer(4)(_.writeInt((i.getLong(JulianFields.JULIAN_DAY) - 2451545).toInt)))
  implicit val Timestamp = instance[LocalDateTime]("timestamp_recv", t => java.sql.Timestamp.valueOf(t).toString, (i, c) => TimeUtils.writeTimestamp(i))


}

/*
 * Helpers for converting strings into bytes (i.e., for Postgres requests).
 */
object StringValueEncoder {
  def encode(value: Any): ChannelBuffer = {
    val result = ChannelBuffers.dynamicBuffer()

    if (value == null || value == None) {
      result.writeInt(-1)
    } else {
      result.writeBytes(convertValue(value).toString.getBytes(Charsets.Utf8))
    }

    result
  }

  def convertValue[A](value:A)(implicit mf:Manifest[A]):Any = {
    value match {
        //need to use a proper format for this
      case zdt:ZonedDateTime => zdt.toOffsetDateTime.toString
      case m:collection.Map[_, _] => { // this is an hstore, so turn it into one
        def escape(s:String):String = {
          s.replace("\\", "\\\\").replace("\"", "\\\"")
        }
        m.map { case (k:String, v:String) =>
          """"%s" => "%s"""".format(escape(k), escape(v))
        }.mkString(",")
      }
      case Some(v) => v
      case _ => value
    }
  }
}

object HStoreStringParser extends RegexParsers {
  def term:Parser[String] = "\"" ~ """([^"\\]*(\\.[^"\\]*)*)""".r ~ "\"" ^^ {
    case o~value~c => value.replace("\\\"", "\"").replace("\\\\", "\\")
  }

  def item:Parser[(String, String)] = term ~ "=>" ~ term ^^ { case key~arrow~value => (key, value) }

  def items:Parser[Map[String, String]] = repsep(item, ", ") ^^ { l => l.toMap }

  def apply(input:String):Option[Map[String, String]] = parseAll(items, input) match {
    case Success(result, _) => Some(result)
    case failure:NoSuccess => None
  }
}