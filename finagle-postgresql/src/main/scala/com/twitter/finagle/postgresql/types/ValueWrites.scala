package com.twitter.finagle.postgresql.types

import java.nio.CharBuffer
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlUnsupportedError
import com.twitter.finagle.postgresql.Types
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
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
 * | CHARACTER(n) | [[String]] |
 * | DATE (date) | [[java.time.LocalDate]] |
 * | DOUBLE (float8) | [[Double]] |
 * | INET | [[Inet]] ([[java.net.InetAddress]] and a subnet) |
 * | INTEGER (int, int4) | [[Int]] |
 * | JSON | [[String]] or [[Json]] |
 * | JSONB | [[Json]] |
 * | NUMERIC (decimal) | [[BigDecimal]] |
 * | REAL (float4) | [[Float]] |
 * | SMALLINT (int2) | [[Short]] and [[Byte]] (since Postgres doesn't have int1) |
 * | TEXT | [[String]] |
 * | TIMESTAMP | [[java.time.Instant]] |
 * | TIMESTAMP WITH TIME ZONE | [[java.time.Instant]] |
 * | UUID | [[java.util.UUID]] |
 * | VARCHAR | [[String]] |
 *
 * @see [[ValueReads]]
 * @see [[PgType]]
 */
trait ValueWrites[T] {

  /**
   * Encode a value to Postgres' wire representation for a particular Postgres type.
   *
   * @note It is the responsability of the caller to ensure that the Postgres type is accepted by this implementation.
   *
   * @param tpe the Postgres type to encode the value into.
   * @param value the value to encode.
   * @param charset when applicable, the character set to use when encoding.
   * @return the encoded value
   */
  def writes(tpe: PgType, value: T, charset: Charset): WireValue

  /**
   * Returns true when this implementation is able to encode values of the provided Postgres type.
   * Returns false otherwise.
   *
   * @param tpe the Postgres type to check.
   * @return true if this implementation can encode values for the type, false otherwise.
   */
  def accepts(tpe: PgType): Boolean

  /**
   * Returns a `ValueWrites` instance that will use `this` if it accepts the type, otherwise
   * will delegate to `that`.
   *
   * @param that the instance to delegate to when `this` does not accept the provided type.
   * @return a `ValueWrites` instance that will use `this` if it accepts the type, otherwise
   *         will delegate to `that`.
   * @see [[ValueWrites.or]]
   */
  def orElse(that: ValueWrites[T]): ValueWrites[T] =
    ValueWrites.or(this, that)

}

object ValueWrites {

  def simple[T](expect: PgType*)(write: (PgBuf.Writer, T) => PgBuf.Writer): ValueWrites[T] = new ValueWrites[T] {
    val accept: Set[PgType] = expect.toSet

    override def writes(tpe: PgType, value: T, charset: Charset): WireValue =
      WireValue.Value(write(PgBuf.writer, value).build)

    override def accepts(tpe: PgType): Boolean = accept(tpe)
  }

  /**
   * Define a `ValueWrites[B]` in terms of `ValueWrites[A]` and `B => A`.
   */
  def by[A, B](f: B => A)(implicit writesA: ValueWrites[A]): ValueWrites[B] = new ValueWrites[B] {
    override def writes(tpe: PgType, value: B, charset: Charset): WireValue =
      writesA.writes(tpe, f(value), charset)
    override def accepts(tpe: PgType): Boolean =
      writesA.accepts(tpe)
  }

  /**
   * If it accepts the given [[PgType]], uses `first` to read the value, otherwise, use `second`.
   *
   * @return an instance of [[ValueWrites[T]] that uses `first` if it accepts the [[PgType]], otherwise uses `second`.
   */
  def or[T](first: ValueWrites[T], second: ValueWrites[T]): ValueWrites[T] = new ValueWrites[T] {
    override def writes(tpe: PgType, value: T, charset: Charset): WireValue = {
      val w = if (first.accepts(tpe)) first else second
      w.writes(tpe, value, charset)
    }

    override def accepts(tpe: PgType): Boolean =
      first.accepts(tpe) || second.accepts(tpe)
  }

  /**
   * Returns a `ValueWrites[Option[T]]` that writes `NULL` when the value is `None` and delegates to the underlying
   * instance when the value is `Some`.
   */
  implicit def optionWrites[T](implicit twrites: ValueWrites[T]): ValueWrites[Option[T]] = new ValueWrites[Option[T]] {
    override def writes(tpe: PgType, value: Option[T], charset: Charset): WireValue =
      value match {
        case Some(v) => twrites.writes(tpe, v, charset)
        case None => WireValue.Null
      }
    override def accepts(tpe: PgType): Boolean = twrites.accepts(tpe)
  }

  /**
   * Returns a [[ValueWrites]] able to write a collection of [T] to a Postgres array type.
   *
   * For example, this can produce [[ValueWrites[List[Int]]] for the [[PgType.Int4Array]] type.
   */
  implicit def traversableWrites[F[X] <: Iterable[X], T](implicit twrites: ValueWrites[T]): ValueWrites[F[T]] =
    new ValueWrites[F[T]] {

      def emptyArray(oid: Types.Oid): PgArray =
        PgArray(0, 0, oid, IndexedSeq.empty, IndexedSeq.empty)
      override def writes(tpe: PgType, values: F[T], charset: Charset): WireValue = {
        val underlying = tpe.kind match {
          case Kind.Array(underlying) => underlying
          case _ => throw new PgSqlClientError(
              s"Type ${tpe.name} is not an array type and cannot be written as such." +
                s" Note that this may be because you're trying to write a multi-dimensional array which isn't supported."
            )
        }
        val data = values.map(v => twrites.writes(underlying, v, charset)).toIndexedSeq
        val pgArray =
          if (data.isEmpty) emptyArray(underlying.oid)
          else {
            PgArray(
              dimensions = 1,
              dataOffset = 0,
              elemType = underlying.oid,
              arrayDims = IndexedSeq(PgArrayDim(data.length, 1)),
              data = data,
            )
          }
        WireValue.Value(PgBuf.writer.array(pgArray).build)
      }

      override def accepts(tpe: PgType): Boolean =
        tpe.kind match {
          case Kind.Array(underlying) => twrites.accepts(underlying)
          case _ => false
        }
    }

  /**
   * Writes [[BigDecimal]] to [[PgType.Numeric]].
   */
  implicit lazy val writesBigDecimal: ValueWrites[BigDecimal] = simple(PgType.Numeric) { (w, bd) =>
    w.numeric(PgNumeric.bigDecimalToNumeric(bd))
  }

  /**
   * Writes [[Boolean]] to [[PgType.Bool]].
   */
  implicit lazy val writesBoolean: ValueWrites[Boolean] = simple(PgType.Bool)((w, t) => w.byte(if (t) 1 else 0))

  /**
   * Writes [[Buf]] to [[PgType.Bytea]].
   */
  implicit lazy val writesBuf: ValueWrites[Buf] = simple(PgType.Bytea)(_.buf(_))

  /**
   * Writes [[Byte]] to [[PgType.Int2]].
   *
   * Postgres does not have a numeric 1-byte data type. So we use 2-byte value and check bounds.
   * NOTE: Postgres does have a 1-byte data type (i.e.: "char" with quotes),
   * but it's very tricky to use to store numbers, so it's unlikely to be useful in practice.
   *
   * @see https://www.postgresql.org/docs/current/datatype-numeric.html
   * @see https://dba.stackexchange.com/questions/159090/how-to-store-one-byte-integer-in-postgresql
   */
  implicit lazy val writesByte: ValueWrites[Byte] = simple(PgType.Int2)((w, b) => w.short(b.toShort))

  /**
   * Writes [[Double]] to [[PgType.Float8]].
   */
  implicit lazy val writesDouble: ValueWrites[Double] = simple(PgType.Float8)(_.double(_))

  /**
   * Writes [[Float]] to [[PgType.Float4]].
   */
  implicit lazy val writesFloat: ValueWrites[Float] = simple(PgType.Float4)(_.float(_))

  /**
   * Writes [[Inet]] to [[PgType.Inet]].
   */
  implicit lazy val writesInet: ValueWrites[Inet] = simple(PgType.Inet)(_.inet(_))

  /**
   * Writes [[java.time.Instant]] to [[PgType.Timestamptz]] or [[PgType.Timestamp]].
   */
  implicit lazy val writesInstant: ValueWrites[java.time.Instant] = simple(PgType.Timestamptz, PgType.Timestamp) {
    (w, instant) =>
      // NOTE: we skip going through Timestamp.Micros since we never write anything else
      w.long(PgTime.instantAsUsecOffset(instant))
  }

  /**
   * Writes [[Int]] to [[PgType.Int4]].
   */
  implicit lazy val writesInt: ValueWrites[Int] = simple(PgType.Int4)(_.int(_))

  /**
   * Writes [[Json]] to [[PgType.Json]] or [[PgType.Jsonb]].
   */
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

  /**
   * Writes [[Long]] to [[PgType.Int8]].
   */
  implicit lazy val writesLong: ValueWrites[Long] = simple(PgType.Int8)(_.long(_))

  /**
   * Writes [[java.time.LocalDate]] to [[PgType.Date]].
   */
  implicit lazy val writesLocalDate: ValueWrites[java.time.LocalDate] = simple(PgType.Date) { (w, date) =>
    w.int(PgDate.localDateAsEpochDayOffset(date))
  }

  /**
   * Writes [[Short]] to [[PgType.Int2]].
   */
  implicit lazy val writesShort: ValueWrites[Short] = simple(PgType.Int2)(_.short(_))

  /**
   * Writes [[String]] to any of [[PgType.Text]], [[PgType.Json]],
   * [[PgType.Varchar]], [[PgType.Bpchar]], [[PgType.Name]], [[PgType.Unknown]].
   */
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

  /**
   * Writes [[java.util.UUID]] to [[PgType.Uuid]].
   */
  implicit lazy val writesUuid: ValueWrites[java.util.UUID] = simple(PgType.Uuid) { (w, uuid) =>
    w.long(uuid.getMostSignificantBits).long(uuid.getLeastSignificantBits)
  }
}
