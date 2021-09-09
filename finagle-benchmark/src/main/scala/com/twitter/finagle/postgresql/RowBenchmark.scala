package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.{PgType, ValueWrites}
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Measurement,
  Mode,
  OutputTimeUnit,
  Scope,
  State,
  Warmup
}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10)
@Measurement(iterations = 10)
class RowBenchmark {

  private[this] val rows: Response.Row = IndexedSeq(
    WireValue.Null,
    ValueWrites.writesInt4.writes(PgType.Int4, 10, StandardCharsets.UTF_8),
    ValueWrites.writesLong.writes(PgType.Int8, 20L, StandardCharsets.UTF_8)
  )

  private[this] val resultSet: ResultSet = ResultSet(
    fields = IndexedSeq(
      Types.FieldDescription("f1", None, None, PgType.Int4.oid, 4, 0, Types.Format.Binary),
      Types.FieldDescription("f2", None, None, PgType.Int4.oid, 4, 0, Types.Format.Binary),
      Types.FieldDescription("f3", None, None, PgType.Int8.oid, 8, 0, Types.Format.Binary)
    ),
    wireRows = Seq(rows),
    parameters = Response.ConnectionParameters(
      Seq(
        BackendMessage.ParameterStatus(BackendMessage.Parameter.TimeZone, "UTC"),
        BackendMessage.ParameterStatus(BackendMessage.Parameter.IntegerDateTimes, "on"),
        BackendMessage.ParameterStatus(BackendMessage.Parameter.ServerEncoding, "UTF8"),
        BackendMessage.ParameterStatus(BackendMessage.Parameter.ClientEncoding, "UTF8"),
      ).toList,
      None
    )
  )

  private[this] val row = resultSet.rows.head

  @Benchmark
  def readRows(): Int = {
    (row.get[Int](1) + row.get[Long](2)).toInt
  }
}
