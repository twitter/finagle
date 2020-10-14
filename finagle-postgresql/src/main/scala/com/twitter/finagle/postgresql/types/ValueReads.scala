package com.twitter.finagle.postgresql.types

import java.nio.charset.Charset

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

  def simple[T](expect: PgType)(f: PgBuf.Reader => T): ValueReads[T] = new ValueReads[T] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[T] = Try(f(new PgBuf.Reader(buf)))
    override def accepts(tpe: PgType): Boolean = expect == tpe
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
        case _ => sys.error("not an array type")
      }
      Try {
        val array = PgBuf.reader(buf).array()
        require(array.dimensions <= 1, s"unsupported dimensions: ${array.dimensions}")
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

  implicit lazy val readsBoolean: ValueReads[Boolean] = simple(PgType.Bool)(_.byte() != 0)
  implicit lazy val readsLong: ValueReads[Long] = simple(PgType.Int8)(_.long())
  implicit lazy val readsShort: ValueReads[Short] = simple(PgType.Int2)(_.short())
  implicit lazy val readsInt: ValueReads[Int] = simple(PgType.Int4)(_.int())
  implicit lazy val readsByte: ValueReads[Byte] = simple(PgType.Char)(_.byte())
  implicit lazy val readsBuf: ValueReads[Buf] = simple(PgType.Bytea)(_.remainingBuf())
  implicit lazy val readsString: ValueReads[String] = new ValueReads[String] {
    override def reads(tpe: PgType, buf: Buf, charset: Charset): Try[String] = {
      // TODO: this uses the lenient codec, we should use the strict one
      Try(Buf.decodeString(buf, charset))
    }

    override def accepts(tpe: PgType): Boolean =
      tpe == PgType.Text ||
        tpe == PgType.Varchar ||
        tpe == PgType.Bpchar || // CHAR(n)
        tpe == PgType.Name || // system identifiers
        tpe == PgType.Unknown // probably used as a fallback to text serialization?
  }
}
