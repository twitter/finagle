package com.twitter.finagle.postgresql.types

import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlUnsupportedError
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

import scala.collection.compat._

/**
 * Typeclass for decoding wire values to Scala/Java types.
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
 * @see [[ValueWrites]]
 * @see [[PgType]]
 */
trait ValueReads[T] {

  /**
   * Decodes a non-null value from the wire.
   */
  def reads(tpe: PgType, buf: Buf, charset: Charset): Try[T]

  /**
   * Produce the value corresponding to the SQL NULL.
   * Note that typically, there is no sensical value (i.e.: there's no Int value to produce for a NULL), thus this
   * method has a default implementation of producing an error.
   */
  def readsNull(tpe: PgType): Try[T] =
    Throw(new IllegalArgumentException(
      s"Type ${tpe.name} has no reasonable null value. If you intended to make this field nullable, you must read it as an Option[T]."
    ))

  /**
   * Decode a potentially `NULL` wire value into the requested scala type.
   * Note that no further validation is done on the passed in `PgType`, the client is expected to have
   * invoked [[accepts()]] first. Not respecting this may lead to successfully reading an invalid value.
   *
   * @param tpe the postgres type to decode (used when the typeclass supports more than one).
   * @param value the value on the wire. This may be NULL.
   * @param charset the server's character set (necessary for decoding strings).
   * @return the decoded value or an exception
   */
  def reads(tpe: PgType, value: WireValue, charset: Charset): Try[T] = value match {
    case WireValue.Null => readsNull(tpe)
    case WireValue.Value(buf) => reads(tpe, buf, charset)
  }

  /**
   * Returns true if this typeclass is able to decode a wire value of the specified type.
   *
   * @param tpe the type of the wire value to decode.
   * @return true if this typeclass can decode the value, false otherwise.
   */
  def accepts(tpe: PgType): Boolean

}

object ValueReads {

  def simple[T](expect: PgType*)(f: PgBuf.Reader => T): ValueReads[T] = new ValueReads[T] {
    val accept: Set[PgType] = expect.toSet
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[T] =
      Try {
        val reader = new PgBuf.Reader(buf)
        val value = f(reader)
        if (reader.remaining != 0) {
          throw new PgSqlClientError(
            s"Reading value of type ${tpe.name} should have consumed the whole value's buffer, but ${reader.remaining} bytes remained."
          )
        }
        value
      }
    override def accepts(tpe: PgType): Boolean = accept(tpe)
  }

  implicit def optionReads[T](implicit treads: ValueReads[T]): ValueReads[Option[T]] = new ValueReads[Option[T]] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[Option[T]] =
      treads.reads(tpe, buf, charset).map(Some(_))
    override def readsNull(tpe: PgType): Try[Option[T]] = Return(None)
    override def accepts(tpe: PgType): Boolean = treads.accepts(tpe)
  }

  implicit def traversableReads[F[_], T](implicit
    treads: ValueReads[T],
    f: Factory[T, F[T]]
  ): ValueReads[F[T]] = new ValueReads[F[T]] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[F[T]] =
      Try {
        val underlying = tpe.kind match {
          case Kind.Array(underlying) => underlying
          case _ => throw new PgSqlClientError(s"Type ${tpe.name} is not an array type and cannot be read as such.")
        }

        val array = PgBuf.reader(buf).array()
        if (array.dimensions > 1) {
          throw PgSqlUnsupportedError(
            s"Multi dimensional arrays are not supported. Expected 0 or 1 dimensions, got ${array.dimensions}"
          )
        }
        val builder = f.newBuilder
        array.data.foreach { value =>
          builder += treads.reads(underlying, value, charset).get()
        }
        builder.result()
      }

    override def accepts(tpe: PgType): Boolean =
      tpe.kind match {
        case Kind.Array(underlying) => treads.accepts(underlying)
        case _ => false
      }
  }

  implicit lazy val readsBigDecimal: ValueReads[BigDecimal] = simple(PgType.Numeric) { reader =>
    PgNumeric.numericToBigDecimal(reader.numeric())
  }
  implicit lazy val readsBoolean: ValueReads[Boolean] = simple(PgType.Bool)(_.byte() != 0)
  implicit lazy val readsBuf: ValueReads[Buf] = simple(PgType.Bytea)(_.remainingBuf())
  implicit lazy val readsByte: ValueReads[Byte] = simple(PgType.Char)(_.byte())
  implicit lazy val readsDouble: ValueReads[Double] = simple(PgType.Float8)(_.double())
  implicit lazy val readsFloat: ValueReads[Float] = simple(PgType.Float4)(_.float())
  implicit lazy val readsInstant: ValueReads[java.time.Instant] =
    simple(PgType.Timestamptz, PgType.Timestamp) { reader =>
      reader.timestamp() match {
        case Timestamp.NegInfinity | Timestamp.Infinity =>
          throw PgSqlUnsupportedError("-Infinity and Infinity timestamps cannot be read as java.time.Instant.")
        case Timestamp.Micros(offset) => PgTime.usecOffsetAsInstant(offset)
      }
    }
  implicit lazy val readsInet: ValueReads[Inet] = simple(PgType.Inet)(_.inet())
  implicit lazy val readsInt: ValueReads[Int] = simple(PgType.Int4)(_.int())
  implicit lazy val readsJson: ValueReads[Json] = new ValueReads[Json] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[Json] =
      tpe match {
        case PgType.Json => Return(Json(buf, charset))
        case PgType.Jsonb =>
          buf.get(0) match {
            case 1 => Return(Json(buf.slice(1, buf.length), charset))
            case _ => Throw(PgSqlUnsupportedError("Only JSONB version 1 is supported"))
          }
        case _ => Throw(PgSqlUnsupportedError(s"readsJson does not support type ${tpe.name}"))
      }

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Jsonb || tpe == PgType.Json
  }
  implicit lazy val readsLong: ValueReads[Long] = simple(PgType.Int8)(_.long())
  implicit lazy val readsShort: ValueReads[Short] = simple(PgType.Int2)(_.short())
  implicit lazy val readsString: ValueReads[String] = new ValueReads[String] {
    def strictDecoder(charset: Charset) =
      charset.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[String] =
      Try(strictDecoder(charset).decode(Buf.ByteBuffer.Owned.extract(buf)).toString)

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Text ||
        tpe == PgType.Json ||
        tpe == PgType.Varchar ||
        tpe == PgType.Bpchar || // CHAR(n)
        tpe == PgType.Name || // system identifiers
        tpe == PgType.Unknown // probably used as a fallback to text serialization?
  }
  implicit lazy val readsUuid: ValueReads[java.util.UUID] = simple(PgType.Uuid) { reader =>
    new java.util.UUID(reader.long(), reader.long())
  }

}
