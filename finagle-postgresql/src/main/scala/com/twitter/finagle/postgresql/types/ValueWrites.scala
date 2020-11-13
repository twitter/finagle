package com.twitter.finagle.postgresql.types

import java.nio.charset.Charset

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

  implicit lazy val writesBigDecimal: ValueWrites[BigDecimal] = unimplemented
  implicit lazy val writesBoolean: ValueWrites[Boolean] = simple(PgType.Bool)((w, t) => w.byte(if (t) 1 else 0))
  implicit lazy val writesBuf: ValueWrites[Buf] = simple(PgType.Bytea)(_.buf(_))
  implicit lazy val writesByte: ValueWrites[Byte] = unimplemented
  implicit lazy val writesDouble: ValueWrites[Double] = simple(PgType.Float8)(_.double(_))
  implicit lazy val writesFloat: ValueWrites[Float] = simple(PgType.Float4)(_.float(_))
  implicit lazy val writesInet: ValueWrites[Inet] = simple(PgType.Inet)(_.inet(_))
  implicit lazy val writesInstant: ValueWrites[java.time.Instant] = unimplemented
  implicit lazy val writesInt: ValueWrites[Int] = simple(PgType.Int4)(_.int(_))
  implicit lazy val writesJson: ValueWrites[Json] = unimplemented
  implicit lazy val writesLong: ValueWrites[Long] = simple(PgType.Int8)(_.long(_))
  implicit lazy val writesShort: ValueWrites[Short] = simple(PgType.Int2)(_.short(_))
  implicit lazy val writesString: ValueWrites[String] = unimplemented
  implicit lazy val writesUuid: ValueWrites[java.util.UUID] = unimplemented
}
