package com.twitter.finagle.postgresql.types

import java.nio.CharBuffer
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

import com.twitter.finagle.postgresql.PgSqlUnsupportedError
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf

/**
 * Typeclass for encoding Scala/Java types to Postgres wire values.
 *
 * Postgres has its own type system, so the mapping of postgres types to scala types is not 1:1.
 * Furthermore, postgres allows creating custom types (i.e.: commonly enums, but any arbitrary type can effectively
 * be created) which also require their own mapping to scala types.
 *
 * The following built-in types and their corresponding scala / java types are provided:
 *
 * | Postgres Type | Scala / Java Type |
 * | --- | --- |
 * | BIGINT (int8) | [[Long]] |
 * | BOOL | [[Boolean]] |
 * | BYTEA (byte[]) | [[Buf]] |
 * | CHAR | [[Byte]] |
 * | CHARACTER(n) | [[String]] |
 * | DOUBLE (float8) | [[Double]] |
 * | INET | [[Inet]] ([[java.net.InetAddress]] and a subnet) |
 * | INTEGER (int, int4) | [[Int]] |
 * | JSON | [[String]] or [[Json]] |
 * | JSONB | [[Json]] |
 * | NUMERIC (decimal) | [[BigDecimal]] |
 * | REAL (float4) | [[Float]] |
 * | SMALLINT (int2) | [[Short]] |
 * | TEXT | [[String]] |
 * | TIMESTAMP | [[java.time.Instant]] |
 * | TIMESTAMP WITH TIME ZONE | [[java.time.Instant]] |
 * | UUID | [[java.util.UUID]] |
 * | VARCHAR | [[String]] |
 *
 * @see [[PgType]]
 */
trait ValueWrites[T] {

  def writes(tpe: PgType, value: T, charset: Charset): WireValue

  def accepts(tpe: PgType): Boolean

}

object ValueWrites {

  def simple[T](expect: PgType*)(write: (PgBuf.Writer, T) => PgBuf.Writer) = new ValueWrites[T] {
    val accept: Set[PgType] = expect.toSet

    override def writes(tpe: PgType, value: T, charset: Charset): WireValue =
      WireValue.Value(write(PgBuf.writer, value).build)

    override def accepts(tpe: PgType): Boolean = accept(tpe)
  }

  implicit def optionWrites[T](implicit twrites: ValueWrites[T]): ValueWrites[Option[T]] = new ValueWrites[Option[T]] {
    override def writes(tpe: PgType, value: Option[T], charset: Charset): WireValue =
      value match {
        case Some(v) => twrites.writes(tpe, v, charset)
        case None => WireValue.Null
      }
    override def accepts(tpe: PgType): Boolean = twrites.accepts(tpe)
  }

  implicit def traversableWrites[F[X] <: Iterable[X], T](implicit twrites: ValueWrites[T]): ValueWrites[F[T]] = {
    val _ = twrites
    unimplemented
  }

  def unimplemented[T] = new ValueWrites[T] {
    override def writes(tpe: PgType, value: T, charset: Charset): WireValue = ???

    override def accepts(tpe: PgType): Boolean = ???
  }

  implicit lazy val writesBigDecimal: ValueWrites[BigDecimal] = simple(PgType.Numeric) { (w, bd) =>
    w.numeric(PgNumeric.bigDecimalToNumeric(bd))
  }
  implicit lazy val writesBoolean: ValueWrites[Boolean] = simple(PgType.Bool)((w, t) => w.byte(if (t) 1 else 0))
  implicit lazy val writesBuf: ValueWrites[Buf] = simple(PgType.Bytea)(_.buf(_))
  implicit lazy val writesByte: ValueWrites[Byte] = simple(PgType.Char)(_.byte(_))
  implicit lazy val writesDouble: ValueWrites[Double] = simple(PgType.Float8)(_.double(_))
  implicit lazy val writesFloat: ValueWrites[Float] = simple(PgType.Float4)(_.float(_))
  implicit lazy val writesInet: ValueWrites[Inet] = simple(PgType.Inet)(_.inet(_))
  implicit lazy val writesInstant: ValueWrites[java.time.Instant] = simple(PgType.Timestamptz, PgType.Timestamp) { (w, instant) =>
    // NOTE: we skip going through Timestamp.Micros since we never write anything else
    w.long(PgTime.instantAsUsecOffset(instant))
  }
  implicit lazy val writesInt: ValueWrites[Int] = simple(PgType.Int4)(_.int(_))
  implicit lazy val writesJson: ValueWrites[Json] = new ValueWrites[Json] {
    // TODO: Json is really only meant for reading...
    override def writes(tpe: PgType, json: Json, charset: Charset): WireValue = {
      val buf = tpe match {
        case PgType.Json => json.value
        case PgType.Jsonb => Buf.ByteArray(1).concat(json.value)
        case _ => throw new PgSqlUnsupportedError(s"readsJson does not support type ${tpe.name}")
      }
      WireValue.Value(buf)
    }

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Json || tpe == PgType.Jsonb
  }
  implicit lazy val writesLong: ValueWrites[Long] = simple(PgType.Int8)(_.long(_))
  implicit lazy val writesShort: ValueWrites[Short] = simple(PgType.Int2)(_.short(_))
  implicit lazy val writesString: ValueWrites[String] = new ValueWrites[String] {
    def strictEncoder(charset: Charset) =
      charset.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)

    override def writes(tpe: PgType, value: String, charset: Charset): WireValue =
      WireValue.Value(Buf.ByteBuffer.Owned(strictEncoder(charset).encode(CharBuffer.wrap(value))))

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Text ||
        tpe == PgType.Json ||
        tpe == PgType.Varchar ||
        tpe == PgType.Bpchar || // CHAR(n)
        tpe == PgType.Name || // system identifiers
        tpe == PgType.Unknown // probably used as a fallback to text serialization?
  }
  implicit lazy val writesUuid: ValueWrites[java.util.UUID] = simple(PgType.Uuid) { (w,uuid) =>
    w.long(uuid.getMostSignificantBits).long(uuid.getLeastSignificantBits)
  }
}
