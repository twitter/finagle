package com.twitter.finagle.postgres

import java.nio.charset.Charset

import com.twitter.finagle.postgres.messages.{DataRow, Field}
import com.twitter.finagle.postgres.values.Value
import com.twitter.util.Try
import org.jboss.netty.buffer.ChannelBuffer

/*
 * Convenience wrapper around a set of row values. Supports lookup by name.
 */
class Row(val fields: IndexedSeq[String], val vals: IndexedSeq[Value[Any]]) {
  private[this] val indexMap = fields.zipWithIndex.toMap

  def getOption[A](name: String)(implicit mf: Manifest[A]): Option[A] = {
    indexMap.get(name).map(vals(_)) match {
      case Some(Value(x)) => Some(x.asInstanceOf[A])
      case _ => None
    }
  }

  def get[A](name: String)(implicit mf:Manifest[A]):A = {
    getOption[A](name) match {
      case Some(x) => x
      case _ => throw new IllegalStateException("Expected type " + mf.toString)
    }
  }

  def getOrElse[A](name: String, default: => A)(implicit mf:Manifest[A]):A = {
    getOption[A](name) match {
      case Some(x) => x
      case _ => default
    }
  }

  def get(index: Int): Value[Any] = vals(index)

  def values(): IndexedSeq[Value[Any]] = vals

  override def toString = "{ fields='" + fields.toString + "', rows='" + vals.toString + "'}"
}

/*
 * A row reader that implements the reader monad pattern and allows
 * to build simple and reusable readers over Postgres Row entity.
 *
 * @tparam A readers' output type
 */
trait RowReader[A] { self =>
  def apply(row: Row): A

  def flatMap[B](fn: A => RowReader[B]) = new RowReader[B] {
    def apply(row: Row) = fn(self(row))(row)
  }

  def map[B](fn: A => B) = new RowReader[B] {
    def apply(row: Row) = fn(self(row))
  }
}

/*
 * A reader that reads a name-specified field from a row.
 */
object RequiredField {
  def apply[A](name: String)(implicit mf: Manifest[A]) = new RowReader[A] {
    def apply(row: Row): A = row.get[A](name)
  }
}

/**
 * A reader that reads an optional, name-specified field from a row.
 */
object OptionalField {
  def apply[A](name: String)(implicit mf: Manifest[A]) = new RowReader[Option[A]] {
    def apply(row: Row): Option[A] = row.getOption[A](name)
  }
}

sealed trait QueryResponse

case class OK(affectedRows: Int) extends QueryResponse

case class ResultSet(rows: List[Row]) extends QueryResponse

/*
 * Helper object to generate ResultSets for responses with custom types.
 */
object ResultSet {
  def apply(
      fieldNames: IndexedSeq[String],
      charset: Charset,
      fieldParsers: IndexedSeq[((ChannelBuffer, Charset)) => Try[Value[Any]]],
      rows: List[DataRow]) = {
    new ResultSet(rows.map(dataRow => new Row(fieldNames, dataRow.data.zip(fieldParsers).map({
      case (d, p) =>
        if (d == null)
          null
        else
          p(d, charset)
            .getOrElse(null)
    }))))
  }
}
