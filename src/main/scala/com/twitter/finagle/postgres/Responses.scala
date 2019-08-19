package com.twitter.finagle.postgres

import java.nio.charset.Charset

import com.twitter.finagle.postgres.messages.{DataRow, Field}
import com.twitter.finagle.postgres.values.ValueDecoder
import com.twitter.util.Try
import Try._
import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.postgres.PostgresClient.TypeSpecifier
import com.twitter.finagle.postgres.codec.NullValue
import io.netty.buffer.ByteBuf

import scala.language.existentials

// capture all common format data for a set of rows to reduce repeated references
case class RowFormat(
  indexMap: Map[String, Int],
  formats: Array[Short],
  oids: Array[Int],
  dataTypes: Map[Int, TypeSpecifier],
  receives: PartialFunction[String, ValueDecoder[T] forSome {type T}],
  charset: Charset
) {
  @inline final def recv(index: Int) = dataTypes(oids(index)).receiveFunction
  @inline final def defaultDecoder(index: Int) = receives.applyOrElse(recv(index), (_: String) => ValueDecoder.never)
}

trait Row {
  def getOption[T](name: String)(implicit decoder: ValueDecoder[T]): Option[T]
  def getOption[T](index: Int)(implicit decoder: ValueDecoder[T]): Option[T]
  def get[T](name: String)(implicit decoder: ValueDecoder[T]): T
  def get[T](index: Int)(implicit decoder: ValueDecoder[T]): T
  def getTry[T](name: String)(implicit decoder: ValueDecoder[T]): Try[T]
  def getTry[T](index: Int)(implicit decoder: ValueDecoder[T]): Try[T]
  def getOrElse[T](name: String, default: => T)(implicit decoder: ValueDecoder[T]): T
  def getOrElse[T](index: Int, default: => T)(implicit decoder: ValueDecoder[T]): T
  def getAnyOption(name: String): Option[Any]
  def getAnyOption(index: Int): Option[Any]
}

object Row {
  def apply(values: Array[Option[ByteBuf]], rowFormat: RowFormat): Row = RowImpl(values, rowFormat)
}

/*
 * Convenience wrapper around a set of row values. Supports lookup by name.
 */
case class RowImpl(
  values: Array[Option[ByteBuf]],
  rowFormat: RowFormat
) extends Row {

  final def getOption[T](name: String)(implicit decoder: ValueDecoder[T]): Option[T] =
    rowFormat.indexMap.get(name).flatMap(i => getOption[T](i))


  final def getOption[T](index: Int)(implicit decoder: ValueDecoder[T]): Option[T] = for {
    buffer <- values(index)
    format =  rowFormat.formats(index)
    value  <- if(format == 0)
                decoder.decodeText(rowFormat.recv(index), buffer.toString(rowFormat.charset)).toOption
              else
                decoder.decodeBinary(rowFormat.recv(index), buffer.duplicate(), rowFormat.charset).toOption
  } yield value

  /**
    * Danger zone! See get[T](Int) below
    */
  final def get[T](name: String)(implicit decoder: ValueDecoder[T]): T =
    get(rowFormat.indexMap(name))

  /**
    * Danger zone! If [[T]] is primitive, and the column is NULL, we explicitly cast null to [[T]] which will result
    * in unexpected results:
    *
    * null.asInstanceOf[Int]     // 0
    * null.asInstanceOf[Boolean] // false
    *
    * And so forth. Therefore, use of `get` is discouraged, with `getOption` or `getTry` preferred.
    */
  final def get[T](index: Int)(implicit decoder: ValueDecoder[T]): T = getOption[T](index).getOrElse(null.asInstanceOf[T])

  final def getTry[T](name: String)(implicit decoder: ValueDecoder[T]): Try[T] =
    rowFormat.indexMap.get(name).orThrow(new NoSuchElementException(name)).flatMap(getTry[T])

  final def getTry[T](index: Int)(implicit decoder: ValueDecoder[T]): Try[T] = for {
    bufferOpt  <- Try(values(index))
    buffer     <- bufferOpt orThrow NullValue
    format     <- Try(rowFormat.formats(index))
    recv       <- Try(rowFormat.recv(index))
    value      <- if(format == 0)
                    decoder.decodeText(recv, buffer.toString(rowFormat.charset))
                  else
                    decoder.decodeBinary(recv, buffer.duplicate(), rowFormat.charset)
  } yield value

  final def getOrElse[T](index: Int, default: => T)(implicit decoder: ValueDecoder[T]): T =
    getTry[T](index).getOrElse(default)

  final def getOrElse[T](name: String, default: => T)(implicit decoder: ValueDecoder[T]): T =
    getTry[T](name).getOrElse(default)

  final def getAnyOption(index: Int): Option[Any] = for {
    buffer  <- values(index)
    format  =  rowFormat.formats(index)
    decoder =  rowFormat.defaultDecoder(index)
    value  <- if(format == 0)
      decoder.decodeText(rowFormat.recv(index), buffer.toString(rowFormat.charset)).toOption
    else
      decoder.decodeBinary(rowFormat.recv(index), buffer.duplicate(), rowFormat.charset).toOption
  } yield value

  final def getAnyOption(name: String): Option[Any] =
    rowFormat.indexMap.get(name).flatMap(i => getAnyOption(i))
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
  def apply[A : ValueDecoder](name: String) = new RowReader[A] {
    def apply(row: Row): A = row.get[A](name)
  }
}

/**
 * A reader that reads an optional, name-specified field from a row.
 */
object OptionalField {
  def apply[A : ValueDecoder](name: String) = new RowReader[Option[A]] {
    def apply(row: Row): Option[A] = row.getOption[A](name)
  }
}

sealed trait QueryResponse

case class OK(affectedRows: Int) extends QueryResponse

case class ResultSet(rows: AsyncStream[Row]) extends QueryResponse

/*
 * Helper object to generate ResultSets for responses with custom types.
 */
object ResultSet {
  def apply(
    fields: Array[Field],
    charset: Charset,
    dataRows: AsyncStream[DataRow],
    types: Map[Int, TypeSpecifier],
    receives: PartialFunction[String, ValueDecoder[T] forSome { type T }]
  ): ResultSet = {
    val (indexMap, formats, oids) = {
      val l = fields.length
      val stringIndex = new Array[(String, Int)](l)
      val formats = new Array[Short](l)
      val oids = new Array[Int](l)
      var i = 0
      while(i < l) {
        val Field(name, format, dataType) = fields(i)
        stringIndex(i) = (name, i)
        formats(i) = format
        oids(i) = dataType
        i += 1
      }
      (stringIndex.toMap, formats, oids)
    }

    val rowFormat = RowFormat(indexMap, formats, oids, types, receives, charset)

    val rows = dataRows.map {
      dataRow => Row(
        values = dataRow.data,
        rowFormat = rowFormat
      )
    }

    ResultSet(rows)
  }
}
