package com.twitter.finagle.postgresql.types

import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlUnsupportedError
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf
import scala.collection.compat._

/**
 * Typeclass for decoding wire values to Scala/Java types.
 *
 * Postgres has its own type system, so the mapping of postgres types to scala types is not 1:1.
 * Furthermore, postgres allows creating custom types (i.e.: commonly enums, but any arbitrary type can effectively
 * be created) which also require their own mapping to scala types.
 *
 * The following built-in types and their corresponding scala / java types are provided
 * (read this table as "Postgres Type X can be read into Scala / Java Type Y"):
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
 * | INTEGER (int, int4) | [[Int]] and [[Long]] |
 * | JSON | [[String]] or [[Json]] |
 * | JSONB | [[Json]] |
 * | NUMERIC (decimal) | [[BigDecimal]] |
 * | REAL (float4) | [[Float]] and [[Double]] |
 * | SMALLINT (int2) | [[Short]], [[Int]] and [[Long]]. As well as [[Byte]] (bounds are checked), since Postgres doesn't have `int1`. |
 * | TEXT | [[String]] |
 * | TIMESTAMP | [[java.time.Instant]] |
 * | TIMESTAMP WITH TIME ZONE | [[java.time.Instant]] |
 * | UUID | [[java.util.UUID]] |
 * | VARCHAR | [[String]] |
 *
 * @note numeric types don't have the same correspondence for reading and writing.
 *
 * @see [[ValueWrites]]
 * @see [[PgType]]
 */
trait ValueReads[@specialized T] {

  /**
   * Decodes a non-null value from the wire, throwing an exception if the decoding fails.
   */
  def reads(tpe: PgType, buf: Buf, charset: Charset): T

  /**
   * Produce the value corresponding to the SQL NULL.
   * Note that typically, there is no sensical value (i.e.: there's no Int value to produce for a NULL), thus this
   * method has a default implementation of producing an error.
   */
  def readsNull(tpe: PgType): T =
    throw new IllegalArgumentException(
      s"Type ${tpe.name} has no reasonable null value. If you intended to make this field nullable, you must read it as an Option[T]."
    )

  /**
   * Decode a potentially `NULL` wire value into the requested scala type.
   * Note that no further validation is done on the passed in `PgType`, the client is expected to have
   * invoked [[accepts]] first. Not respecting this may lead to successfully reading an invalid value.
   *
   * @param tpe the postgres type to decode (used when the typeclass supports more than one).
   * @param value the value on the wire. This may be NULL.
   * @param charset the server's character set (necessary for decoding strings).
   * @return the decoded value or an exception
   */
  def reads(tpe: PgType, value: WireValue, charset: Charset): T = value match {
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

  /**
   * Returns a `ValueReads` instance that will use `this` if it accepts the type, otherwise
   * will delegate to `that`.
   *
   * @param that the instance to delegate to when `this` does not accept the provided type.
   * @return a `ValueReads` instance that will use `this` if it accepts the type, otherwise
   *         will delegate to `that`.
   * @see [[ValueReads.or]]
   */
  def orElse(that: ValueReads[T]): ValueReads[T] =
    ValueReads.or(this, that)

}

object ValueReads {

  def simpleBuf[@specialized T](expectByteSize: Int, expect: PgType*)(f: Buf => T): ValueReads[T] =
    new ValueReads[T] {
      private[this] final val accept: Set[PgType] = expect.toSet

      override def reads(tpe: PgType, buf: Buf, charset: Charset): T = {
        if (expectByteSize != -1 && buf.length != expectByteSize) {
          throw new PgSqlClientError(
            s"Reading value of type ${tpe.name} expected $expectByteSize but got a buffer of length ${buf.length}"
          )
        }
        f(buf)
      }

      override def accepts(tpe: PgType): Boolean = accept(tpe)
    }

  def simple[@specialized T](expect: PgType*)(f: PgBuf.Reader => T): ValueReads[T] =
    new ValueReads[T] {
      val accept: Set[PgType] = expect.toSet
      override def reads(tpe: PgType, buf: Buf, charset: Charset): T = {
        val reader = PgBuf.Reader(buf)
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

  /**
   * Define a `ValueReads[B]` in terms of `ValueReads[A]` and `A => B`.
   */
  def by[A, B](f: A => B)(implicit readsA: ValueReads[A]): ValueReads[B] = new ValueReads[B] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): B = {
      f(readsA.reads(tpe, buf, charset))
    }
    override def accepts(tpe: PgType): Boolean = readsA.accepts(tpe)
  }

  /**
   * If it accepts the given [[PgType]], uses `first` to read the value, otherwise, use `second`.
   *
   * @return an instance of [[ValueReads[T]] that uses `first` if it accepts the [[PgType]], otherwise uses `second`.
   */
  def or[T](first: ValueReads[T], second: ValueReads[T]): ValueReads[T] = new ValueReads[T] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): T = {
      val r = if (first.accepts(tpe)) first else second
      r.reads(tpe, buf, charset)
    }

    override def accepts(tpe: PgType): Boolean =
      first.accepts(tpe) || second.accepts(tpe)
  }

  /**
   * Returns a `ValueReads[Option[T]]` that reads any `NULL` value as `None` and delegates non-`NULL` values
   * to the underlying `ValueReads` instance.
   */
  implicit def optionReads[T](implicit treads: ValueReads[T]): ValueReads[Option[T]] =
    new ValueReads[Option[T]] {

      override def reads(tpe: PgType, buf: Buf, charset: Charset): Option[T] =
        Some(treads.reads(tpe, buf, charset))
      override def readsNull(tpe: PgType): Option[T] = None
      override def accepts(tpe: PgType): Boolean = treads.accepts(tpe)
    }

  /**
   * Returns a [[ValueReads]] for a collection of [T] from a Postgres array type.
   *
   * For example, this can produce [[ValueReads[List[Int]]] for the [[PgType.Int4Array]] type.
   */
  implicit def traversableReads[F[_], T](
    implicit
    treads: ValueReads[T],
    f: Factory[T, F[T]]
  ): ValueReads[F[T]] = new ValueReads[F[T]] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): F[T] = {
      val underlying = tpe.kind match {
        case Kind.Array(underlying) => underlying
        case _ =>
          throw new PgSqlClientError(
            s"Type ${tpe.name} is not an array type and cannot be read as such.")
      }

      val array = PgBuf.reader(buf).array()
      if (array.dimensions > 1) {
        throw PgSqlUnsupportedError(
          s"Multi dimensional arrays are not supported. Expected 0 or 1 dimensions, got ${array.dimensions}"
        )
      }
      val builder = f.newBuilder
      array.data.foreach { value =>
        builder += treads.reads(underlying, value, charset)
      }
      builder.result()
    }

    override def accepts(tpe: PgType): Boolean =
      tpe.kind match {
        case Kind.Array(underlying) => treads.accepts(underlying)
        case _ => false
      }
  }

  /**
   * Reads [[BigDecimal]] from [[PgType.Numeric]].
   */
  implicit val readsBigDecimal: ValueReads[BigDecimal] = simple(PgType.Numeric) { reader =>
    PgNumeric.numericToBigDecimal(reader.numeric())
  }

  /**
   * Reads [[Boolean]] from [[PgType.Bool]].
   */
  implicit val readsBoolean: ValueReads[Boolean] = simpleBuf(1, PgType.Bool)(_.get(0) != 0)

  /**
   * Reads [[Buf]] from [[PgType.Bytea]].
   */
  implicit val readsBuf: ValueReads[Buf] = simpleBuf(-1, PgType.Bytea)(identity)

  /**
   * Reads [[Byte]] from [[PgType.Int2]].
   *
   * Postgres does not have a numeric 1-byte data type. So we use 2-byte value and check bounds.
   * NOTE: Postgres does have a 1-byte data type (i.e.: "char" with quotes),
   * but it's very tricky to use to store numbers, so it's unlikely to be useful in practice.
   *
   * @see https://www.postgresql.org/docs/current/datatype-numeric.html
   * @see https://dba.stackexchange.com/questions/159090/how-to-store-one-byte-integer-in-postgresql
   */
  implicit val readsByte: ValueReads[Byte] = simpleBuf(java.lang.Short.BYTES, PgType.Int2) { buf =>
    val shortVal = readShortBE(buf)
    if (!shortVal.isValidByte)
      throw new PgSqlClientError(
        s"int2 value is out of range for reading as a Byte: $shortVal is not within [${Byte.MinValue},${Byte.MaxValue}]. Consider reading as Short instead."
      )
    shortVal.toByte
  }

  private def readShortBE(buf: Buf): Short = {
    ((buf.get(0) & 0xff) << 8 |
      (buf.get(1) & 0xff)).toShort
  }

  private def readIntBE(buf: Buf): Int = {
    (buf.get(0) & 0xff) << 24 |
      (buf.get(1) & 0xff) << 16 |
      (buf.get(2) & 0xff) << 8 |
      (buf.get(3) & 0xff)
  }

  private def readLongBE(buf: Buf): Long = {
    (buf.get(0) & 0xff).toLong << 56 |
      (buf.get(1) & 0xff).toLong << 48 |
      (buf.get(2) & 0xff).toLong << 40 |
      (buf.get(3) & 0xff).toLong << 32 |
      (buf.get(4) & 0xff).toLong << 24 |
      (buf.get(5) & 0xff).toLong << 16 |
      (buf.get(6) & 0xff).toLong << 8 |
      (buf.get(7) & 0xff).toLong
  }

  /**
   * Reads [[Float]] from [[PgType.Float4]].
   */
  implicit val readsFloat: ValueReads[Float] = simpleBuf(java.lang.Integer.BYTES, PgType.Float4) {
    buf =>
      val intValue = readIntBE(buf)
      java.lang.Float.intBitsToFloat(intValue)
  }

  /**
   * Reads [[Double]] from [[PgType.Float8]].
   */
  val readsFloat8: ValueReads[Double] = simpleBuf(java.lang.Long.BYTES, PgType.Float8) { buf =>
    val longValue = readLongBE(buf)
    java.lang.Double.longBitsToDouble(longValue)
  }

  /**
   * Reads [[Double]] from [[PgType.Float8]] or [[PgType.Float4]].
   */
  implicit val readsDouble: ValueReads[Double] =
    readsFloat8.orElse(by[Float, Double](_.toDouble)(readsFloat))

  /**
   * Reads [[java.time.Instant]] from [[PgType.Timestamptz]] or [[PgType.Timestamp]].
   */
  implicit val readsInstant: ValueReads[java.time.Instant] =
    simple(PgType.Timestamptz, PgType.Timestamp) { reader =>
      reader.timestamp() match {
        case Timestamp.NegInfinity | Timestamp.Infinity =>
          throw PgSqlUnsupportedError(
            "-Infinity and Infinity timestamps cannot be read as java.time.Instant.")
        case Timestamp.Micros(offset) => PgTime.usecOffsetAsInstant(offset)
      }
    }

  /**
   * Reads [[Inet]] from [[PgType.Inet]].
   */
  implicit val readsInet: ValueReads[Inet] = simple(PgType.Inet)(_.inet())

  /**
   * Reads [[Json]] from [[PgType.Json]] or [[PgType.Jsonb]].
   */
  implicit val readsJson: ValueReads[Json] = new ValueReads[Json] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Json =
      tpe match {
        case PgType.Json => Json(buf, charset)
        case PgType.Jsonb =>
          buf.get(0) match {
            case 1 => Json(buf.slice(1, buf.length), charset)
            case _ => throw PgSqlUnsupportedError("Only JSONB version 1 is supported")
          }
        case _ => throw PgSqlUnsupportedError(s"readsJson does not support type ${tpe.name}")
      }

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Jsonb || tpe == PgType.Json
  }

  implicit val readsLocalDate: ValueReads[java.time.LocalDate] = simple(PgType.Date) { buf =>
    PgDate.dayOffsetAsLocalDate(buf.int())
  }

  /**
   * Reads [[Int]] from [[PgType.Int4]].
   */
  val readsInt4: ValueReads[Int] = simpleBuf(Integer.BYTES, PgType.Int4)(readIntBE)

  /**
   * Reads [[Long]] from [[PgType.Int8]].
   */
  val readsInt8: ValueReads[Long] = simpleBuf(java.lang.Long.BYTES, PgType.Int8)(readLongBE)

  /**
   * Reads [[Short]] from [[PgType.Int2]].
   */
  implicit val readsShort: ValueReads[Short] =
    simpleBuf(java.lang.Short.BYTES, PgType.Int2)(readShortBE)

  /**
   * Reads [[Int]] from [[PgType.Int4]] or [[readsShort]].
   */
  implicit val readsInt: ValueReads[Int] = or(readsInt4, by[Short, Int](_.toInt)(readsShort))

  /**
   * Reads [[Long]] from [[PgType.Int8]] or [[readsInt]].
   */
  implicit val readsLong: ValueReads[Long] = or(readsInt8, by[Int, Long](_.toLong)(readsInt))

  /**
   * Reads [[String]] from any of [[PgType.Text]], [[PgType.Json]],
   * [[PgType.Varchar]], [[PgType.Bpchar]], [[PgType.Name]], [[PgType.Unknown]].
   */
  implicit val readsString: ValueReads[String] = new ValueReads[String] {
    def strictDecoder(charset: Charset): CharsetDecoder =
      charset
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)

    override def reads(tpe: PgType, buf: Buf, charset: Charset): String =
      strictDecoder(charset).decode(Buf.ByteBuffer.Owned.extract(buf)).toString

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Text ||
        tpe == PgType.Json ||
        tpe == PgType.Varchar ||
        tpe == PgType.Bpchar || // CHAR(n)
        tpe == PgType.Name || // system identifiers
        tpe == PgType.Unknown // probably used as a fallback to text serialization?
  }

  /**
   * Like [[readsString]] but is more permissive with encoding errors.
   */
  val readsStringPermissive: ValueReads[String] = new ValueReads[String] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): String =
      Buf.decodeString(buf, charset)

    override def accepts(tpe: PgType): Boolean = readsString.accepts(tpe)
  }

  /**
   * Reads [[java.util.UUID]] from [[PgType.Uuid]].
   */
  implicit val readsUuid: ValueReads[java.util.UUID] = simple(PgType.Uuid) { reader =>
    new java.util.UUID(reader.long(), reader.long())
  }

}
