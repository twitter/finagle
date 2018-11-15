package com.twitter.finagle.mysql

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class StringEncodedRowBenchmark extends StdBenchAnnotations {

  private[this] val rowBuf: Buf = {
    // this was captured from a run of NumericTypeTest
    val rowBytesAsString =
      "1,49,3,49,50,55,5,51,50,55,54,55,7,56,51,56,56,54,48,55,10,50,49,52,55,52,56,51,54,52,55,19,57,50,50,51,51,55,50,48,51,54,56,53,52,55,55,53,56,48,55,4,49,46,54,49,5,49,46,54,49,56,13,49,46,54,49,56,48,51,51,57,56,56,55,53,1,1"
    val bytes = rowBytesAsString.split(",").map(_.toByte)
    Buf.ByteArray.Owned(bytes)
  }

  private[this] val indexMap: Map[String, Int] = Map.empty

  private[this] def newField(id: String, fieldType: Short): Field = Field(
    "catalog",
    "db",
    "table",
    "table",
    id,
    id,
    MysqlCharset.Utf8_general_ci,
    100, // displayLength
    fieldType,
    0.toShort,
    0.toByte
  )

  // these come from NumericTypeType
  private[this] val fields = IndexedSeq(
    newField("boolean", Type.Tiny),
    newField("tinyint", Type.Tiny),
    newField("smallint", Type.Short),
    newField("mediumint", Type.Int24),
    newField("int", Type.Long),
    newField("bigint", Type.LongLong),
    newField("float", Type.Float),
    newField("double", Type.Double),
    newField("decimal", Type.NewDecimal),
    newField("bit", Type.Bit)
  )

  @Benchmark
  def constructorAndValuesWithNumbers: IndexedSeq[Value] =
    new StringEncodedRow(
      rowBuf,
      fields,
      indexMap,
      ignoreUnsigned = true
    ).values

}
