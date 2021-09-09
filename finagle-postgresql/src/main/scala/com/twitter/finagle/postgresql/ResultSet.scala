package com.twitter.finagle.postgresql

import java.nio.charset.Charset
import com.twitter.finagle.postgresql.Response.ConnectionParameters
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.ValueReads
import com.twitter.util.Future
import scala.util.control.NonFatal

final case class Row(
  fields: IndexedSeq[FieldDescription],
  values: IndexedSeq[WireValue],
  charset: Charset,
  columnIndex: Map[String, Int],
) {

  def indexOf(column: String): Option[Int] =
    columnIndex.get(column)

  def isNull(index: Int): Boolean =
    values(index) == WireValue.Null

  def get[@specialized T](index: Int)(implicit treads: ValueReads[T]): T = {
    val field = fields(index)
    val value = values(index)
    field.pgType match {
      case None =>
        throw PgSqlUnsupportedError(
          s"Unsupported type for column index $index named ${field.name}. Unknown oid: ${field.dataType.value}"
        )
      case Some(tpe) =>
        if (!treads.accepts(tpe)) {
          throw PgSqlUnsupportedError(
            s"Cannot decode column index $index named ${field.name} with provided ValueReads; it does not support type ${tpe.name} (oid ${tpe.oid.value})."
          )
        } else {
          try {
            treads.reads(tpe, value, charset)
          } catch {
            case NonFatal(t) =>
              throw new PgSqlClientError(
                s"Error decoding value for column index $index named ${field.name}",
                Some(t))
          }
        }
    }
  }

  def apply[T: ValueReads](column: String): T =
    get(
      indexOf(column)
        .getOrElse(
          throw new PgSqlClientError(
            s"No such column $column. Expected one of ${fields.map(_.name).mkString(", ")}")
        ))
}

final case class ResultSet(
  fields: IndexedSeq[FieldDescription],
  wireRows: Seq[IndexedSeq[WireValue]],
  parameters: ConnectionParameters) {

  val columnIndex: Map[String, Int] = fields.map(_.name).zipWithIndex.toMap

  lazy val rows: Iterable[Row] =
    wireRows.map(columns =>
      Row(fields, columns, parameters.parsedParameters.serverEncoding, columnIndex))
}

object ResultSet {
  def apply(result: Response.ResultSet): Future[ResultSet] =
    result.toSeq
      .map { rows =>
        ResultSet(result.fields, rows, result.parameters)
      }
}
