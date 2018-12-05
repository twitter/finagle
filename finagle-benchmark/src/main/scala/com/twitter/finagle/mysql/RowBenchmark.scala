package com.twitter.finagle.mysql

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class RowBenchmark extends StdBenchAnnotations {
  import RowBenchmark._

  private[this] val stringColumnName = "string"
  private[this] val stringField: Field = newField(stringColumnName, Type.String)
  private[this] val stringValue: Value = StringValue("a string")

  private[this] val booleanColumnName = "boolean"
  private[this] val booleanField: Field = newField(booleanColumnName, Type.Tiny)
  private[this] val booleanValue: Value = ByteValue(1.toByte)

  // we want at least 5 columns to avoid the Map1/2/3/4 optimizations
  private[this] val row: Row = new SimpleRow(
    fields = IndexedSeq(
      stringField,
      booleanField,
      newField("a", Type.String),
      newField("b", Type.String),
      newField("c", Type.String)
    ),
    values = IndexedSeq(
      stringValue,
      booleanValue,
      NullValue,
      NullValue,
      NullValue
    )
  )

  @Benchmark
  def apply: Option[Value] = row(stringColumnName)

  @Benchmark
  def stringOrNull: String = row.stringOrNull(stringColumnName)

  @Benchmark
  def getString: Option[String] = row.getString(stringColumnName)

  @Benchmark
  def booleanOrFalse: Boolean = row.booleanOrFalse(booleanColumnName)

  @Benchmark
  def getBoolean: Option[java.lang.Boolean] = row.getBoolean(booleanColumnName)

}

private object RowBenchmark {

  def newField(columnName: String, fieldType: Short): Field =
    Field(
      catalog = "catalog",
      db = "db",
      table = "table",
      origTable = "table",
      name = columnName,
      origName = columnName,
      charset = MysqlCharset.Utf8_general_ci,
      displayLength = 10,
      fieldType = fieldType,
      flags = 0.toShort,
      decimals = 0.toByte
    )

  // In order to avoid decoding the mysql row we replicate the essence of
  // StringEncodedRow and BinaryEncodedRow
  class SimpleRow(val fields: IndexedSeq[Field], val values: IndexedSeq[Value]) extends Row {
    private[this] val indexMap: Map[String, Int] =
      fields.zipWithIndex.map {
        case (field, i) =>
          field.id -> i
      }.toMap

    def indexOf(columnName: String): Option[Int] = indexMap.get(columnName)

    override protected def indexOfOrSentinel(columnName: String): Int =
      indexMap.getOrElse(columnName, -1)

  }

}
