package com.twitter.finagle.mysql

import com.twitter.finagle.benchmark.StdBenchAnnotations
import java.sql.Timestamp
import java.util.TimeZone
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class TimestampValueBenchmark extends StdBenchAnnotations {

  private[this] final val timeZone =
    TimeZone.getTimeZone("UTC")

  private def rawValue(dateString: String): RawValue =
    RawValue(
      Type.Timestamp,
      MysqlCharset.Utf8_general_ci,
      isBinary = false,
      dateString.getBytes("UTF-8")
    )

  private[this] final val noNanos: RawValue =
    rawValue("2018-04-18 16:30:05")

  private[this] final val withNanos: RawValue =
    rawValue("2018-04-18 16:30:05.123456789")

  private[this] final val paddedNanos: RawValue =
    rawValue("2018-04-18 16:30:05.1")

  @Benchmark
  def fromValueStringNoNanos(): Timestamp =
    TimestampValue.fromValue(noNanos, timeZone).get

  @Benchmark
  def fromValueStringWithNanos(): Timestamp =
    TimestampValue.fromValue(withNanos, timeZone).get

  @Benchmark
  def fromValueStringWithNanosPadded(): Timestamp =
    TimestampValue.fromValue(paddedNanos, timeZone).get

}
