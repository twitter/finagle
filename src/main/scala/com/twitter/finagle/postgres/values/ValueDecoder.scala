package com.twitter.finagle.postgres.values

import java.net.InetAddress
import java.nio.charset.Charset
import java.time._
import java.time.temporal.JulianFields
import java.util.UUID

import com.twitter.util.{Return, Throw, Try}
import org.jboss.netty.buffer.ChannelBuffer


/**
  * Typeclass supporting type-safe decoding of values from PostgreSQL in text and binary formats
  */
trait ValueDecoder[T] {

  /**
    * Attempt to decode the given text with the Postgres data type represented by `recv` into [[T]]
    * @param recv The name of PostgreSQL's receive function for the text's data type
    * @param text The text to be decoded
    * @return A [[Try]] encapsulating the decoding attempt
    */
  def decodeText(recv: String, text: String): Try[T]

  /**
    * Attempt to decode the given bytes with the Postgres data type represented by `recv` into [[T]]
    * @param recv    The name of PostgreSQL's receive function for the text's data type
    * @param bytes   The bytes to be decoded; must be at the correct position (beginning of bytes for this value)
    * @param charset The character set that is being used to communicate with Postgres
    * @return A [[Try]] encapsulating the decoding attempt
    */
  def decodeBinary(recv: String, bytes: ChannelBuffer, charset: Charset): Try[T]

  /**
    * Create a new [[ValueDecoder]] which is the composition of this decoder with the given function
    * @param fn The function which will transform [[T]]
    * @tparam U The type parameter of the new decoder
    * @return A new [[ValueDecoder]] decoding [[U]]
    */
  def map[U](fn: T => U): ValueDecoder[U] = new ValueDecoder[U] {
    def decodeText(recv: String, text: String) = ValueDecoder.this.decodeText(recv, text).map(fn)
    def decodeBinary(recv: String, bytes: ChannelBuffer, charset: Charset) =
      ValueDecoder.this.decodeBinary(recv, bytes, charset).map(fn)
  }
}

object ValueDecoder {

  def apply[T : ValueDecoder] = implicitly[ValueDecoder[T]]

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

  def unknownBinary(t: (ChannelBuffer, Charset)): Try[Any] = Return(Buffers.readBytes(t._1))

  val decoders: PartialFunction[String, ValueDecoder[T] forSome { type T }] = {
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
