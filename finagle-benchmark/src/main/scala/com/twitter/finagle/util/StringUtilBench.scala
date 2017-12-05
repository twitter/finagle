package com.twitter.finagle.util

import com.google.common.base.Splitter
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.util.StringUtil
import java.util
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@Threads(1)
class StringUtilBench extends StdBenchAnnotations {

  val str = "1,2,3,4,5,6,7"

  @Benchmark
  def utilStringSplit(): IndexedSeq[String] = {
    StringUtil.split(str, ',')
  }

  @Benchmark
  def javaStringSplit(): Array[String] = {
    str.split(",")
  }

  val splitter = Splitter.on(',')

  @Benchmark
  def guavaStringSplit(): util.List[String] = {
    splitter.splitToList(str)
  }
}
