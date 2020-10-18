package com.twitter.finagle.postgresql.types

import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlUnsupportedError
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

import scala.collection.generic.CanBuildFrom

trait ValueReads[T] {

  def reads(tpe: PgType, buf: Buf, charset: Charset): Try[T]
  def readsNull(tpe: PgType): Try[T] =
    Throw(new IllegalArgumentException(s"Type ${tpe.name} has no reasonable null value. If you intended to make this field nullable, you must read it as an Option[T]."))

  def reads(tpe: PgType, value: WireValue, charset: Charset): Try[T] = value match {
    case WireValue.Null => readsNull(tpe)
    case WireValue.Value(buf) => reads(tpe, buf, charset)
  }

  def accepts(tpe: PgType): Boolean

}

object ValueReads {

  def simple[T](expect: PgType*)(f: PgBuf.Reader => T): ValueReads[T] = new ValueReads[T] {
    val accept: Set[PgType] = expect.toSet
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[T] =
      Try {
        val reader = new PgBuf.Reader(buf)
        val value = f(reader)
        if(reader.remaining != 0) {
          throw new PgSqlClientError(s"Reading value of type ${tpe.name} should have consumed the whole value's buffer, but ${reader.remaining} bytes remained.")
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

  implicit def traversableReads[F[_], T](implicit treads: ValueReads[T],
   cbf: CanBuildFrom[F[_], T, F[T]]
  ): ValueReads[F[T]] = new ValueReads[F[T]] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[F[T]] = {
      val underlying = tpe.kind match {
        case Kind.Array(underlying) => underlying
        case _ => throw new PgSqlClientError(s"Type ${tpe.name} is not an array type and cannot be read as such.")
      }
      Try {
        val array = PgBuf.reader(buf).array()
        if(array.dimensions > 1) {
          throw PgSqlUnsupportedError(s"Multi dimensional arrays are not supported. Expected 0 or 1 dimensions, got ${array.dimensions}")
        }
        val builder = cbf.apply()
        array.data.foreach { value =>
          builder += treads.reads(underlying, value, charset).get
        }
        builder.result()
      }
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
  implicit lazy val readsInstant: ValueReads[java.time.Instant] = simple(PgType.Timestamptz, PgType.Timestamp) { reader =>
    reader.timestamp() match {
      case Timestamp.NegInfinity | Timestamp.Infinity => throw PgSqlUnsupportedError("-Infinity and Infinity timestamps cannot be read as java.time.Instant.")
      case Timestamp.Micros(offset) => PgTime.usecOffsetAsInstant(offset)
    }
  }
  implicit lazy val readsInt: ValueReads[Int] = simple(PgType.Int4)(_.int())
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
        tpe == PgType.Varchar ||
        tpe == PgType.Bpchar || // CHAR(n)
        tpe == PgType.Name || // system identifiers
        tpe == PgType.Unknown // probably used as a fallback to text serialization?
  }
  implicit lazy val readsUuid: ValueReads[java.util.UUID] = simple(PgType.Uuid) { reader =>
    new java.util.UUID(reader.long(), reader.long())
  }

}
