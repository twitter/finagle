package com.twitter.finagle.postgresql

import java.nio.charset.Charset

import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.PgType
import com.twitter.finagle.postgresql.types.ValueReads
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw

case class Row(
  fields: IndexedSeq[FieldDescription],
  values: IndexedSeq[WireValue],
  charset: Charset,
  columnIndex: Map[String, Int],
) {

  def indexOf(column: String): Option[Int] =
    columnIndex.get(column)

  def isNull(index: Int): Boolean =
    values(index) == WireValue.Null

  def get[T](index: Int)(implicit treads: ValueReads[T]): T = {
    val field = fields(index)
    val value = values(index)
    PgType.byOid(field.dataType) match {
      case None => throw PgSqlUnsupportedError(s"Unsupported type for column index $index named ${field.name}. Unknown oid: ${field.dataType.value}")
      case Some(tpe) =>
        if(!treads.accepts(tpe)) {
          throw PgSqlUnsupportedError(s"Cannot decode column index $index named ${field.name} with provided ValueReads; it does not support type ${tpe.name} (oid ${tpe.oid.value}).")
        } else {
          treads.reads(tpe, value, charset) match {
            case Return(v) => v
            case Throw(t) => throw new PgSqlClientError(s"Error decoding value for column index $index named ${field.name}", Some(t))
          }
        }
    }
  }

  def apply[T: ValueReads](column: String): T =
    get(indexOf(column)
      .getOrElse(throw new PgSqlClientError(s"No such column $column. Expected one of ${fields.map(_.name).mkString(", ")}")))
}

case class ResultSet(fields: IndexedSeq[FieldDescription], wireRows: Seq[IndexedSeq[WireValue]], charset: Charset) {

  val columnIndex: Map[String, Int] = fields.map(_.name).zipWithIndex.toMap

  lazy val rows: Iterable[Row] =
    wireRows.map { columns => Row(fields, columns, charset, columnIndex) }
}
object ResultSet {
  def apply(result: Response.ResultSet, charset: Charset): Future[ResultSet] = {
    result
      .toSeq
      .map { rows =>
        ResultSet(result.fields, rows, charset)
      }
  }
}
