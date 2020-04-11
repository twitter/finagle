package com.twitter.finagle.postgres.values

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.time._
import java.time.temporal.JulianFields
import java.util.UUID

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

import scala.language.existentials






/**
  * Typeclass responsible for encoding a parameter of type T for sending to postgres
  * @tparam T The type which it encodes
  */
trait ValueEncoder[T] {
  def encodeText(t: T): Option[String]
  def encodeBinary(t: T, charset: Charset): Option[ByteBuf]
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

  def apply[T](implicit encoder: ValueEncoder[T]) = encoder

  case class Exported[T](encoder: ValueEncoder[T])

  private val nullParam = {
    val buf = Unpooled.buffer(4)
    buf.writeInt(-1)
    buf
  }

  def instance[T](
    instanceTypeName: String,
    text: T => String,
    binary: (T, Charset) => Option[ByteBuf]
  ): ValueEncoder[T] = new ValueEncoder[T] {
    def encodeText(t: T) = Option(t).map(text)
    def encodeBinary(t: T, c: Charset) = binary(t, c)
    val typeName = instanceTypeName
    val elemTypeName = None
  }

  def encodeText[T](t: T, encoder: ValueEncoder[T], charset: Charset = StandardCharsets.UTF_8) =
    Option(t).flatMap(encoder.encodeText) match {
      case None => nullParam.duplicate()
      case Some(str) =>
        val bytes = str.getBytes(charset)
        val buf = Unpooled.buffer(bytes.length + 4)
        buf.writeInt(bytes.length)
        buf.writeBytes(bytes)
        buf
    }

  def encodeBinary[T](t: T, encoder: ValueEncoder[T], charset: Charset = StandardCharsets.UTF_8) =
    Option(t).flatMap(encoder.encodeBinary(_, charset)) match {
      case None => nullParam.duplicate()
      case Some(inBuf) =>
        inBuf.resetReaderIndex()
        val outBuf = Unpooled.buffer(inBuf.readableBytes() + 4)
        outBuf.writeInt(inBuf.readableBytes())
        outBuf.writeBytes(inBuf)
        outBuf
    }

  private def buffer(capacity: Int)(fn: ByteBuf => Unit) = {
    val cb = Unpooled.buffer(capacity)
    fn(cb)
    cb
  }

  implicit val string: ValueEncoder[String] = instance(
    "text",
    identity,
    (s, c) => Option(s).map(s => Unpooled.wrappedBuffer(s.getBytes(c)))
  )

  implicit val boolean: ValueEncoder[Boolean] = instance(
    "bool",
    b => if(b) "t" else "f",
    (b, c) => Some {
      val buf = Unpooled.buffer(1)
      buf.writeByte(if(b) 1.toByte else 0.toByte)
      buf
    }
  )

  implicit val bytea: ValueEncoder[Array[Byte]] = instance(
    "bytea",
    bytes => "\\x" + bytes.map("%02x".format(_)).mkString,
    (b, c) => Some(Unpooled.copiedBuffer(b))
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
    t => DateTimeUtils.format(t),
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestamp(ts))
  )
  implicit val timestampTz: ValueEncoder[ZonedDateTime] = instance(
    "timestamptz",
    t => DateTimeUtils.format(t),
    (ts, c) => Option(ts).map(ts => DateTimeUtils.writeTimestampTz(ts))
  )
  implicit val instant: ValueEncoder[Instant] = instance(
    "timestamptz",
    i => DateTimeUtils.format(i),
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
    m: Map[String, String] => m.map {
      case (k, v) => (k, Option(v))
    }
  }

  implicit val jsonb: ValueEncoder[JSONB] = instance[JSONB](
    "jsonb",
    j => JSONB.stringify(j),
    (j, c) => {
      val cb = Unpooled.buffer(1 + j.bytes.length)
      cb.writeByte(1)
      cb.writeBytes(j.bytes)
      Some(cb)
    }
  )

  @inline final implicit def option[T](implicit encodeT: ValueEncoder[T]): ValueEncoder[Option[T]] =
    new ValueEncoder[Option[T]] {
      val typeName = encodeT.typeName
      val elemTypeName = encodeT.elemTypeName
      def encodeText(optT: Option[T]) = optT.flatMap(encodeT.encodeText)
      def encodeBinary(tOpt: Option[T], c: Charset) = tOpt.flatMap(t => encodeT.encodeBinary(t, c))
    }

  @inline final implicit def some[T](implicit encodeT: ValueEncoder[T]): ValueEncoder[Some[T]] =
    encodeT.contraMap((s: Some[T]) => s.get)

  implicit object none extends ValueEncoder[None.type] {
    val typeName = "null"
    val elemTypeName = None
    def encodeText(none: None.type) = None
    def encodeBinary(none: None.type, c: Charset) = None
  }
}

trait LowPriorityEncoder {
  implicit def fromExport[T](implicit export: ValueEncoder.Exported[T]): ValueEncoder[T] = export.encoder
}
