package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class RequestBenchmark extends StdBenchAnnotations {

  private[this] val reqUtf8 = Request()
  reqUtf8.contentType = "application/json; charset=utf-8"

  private[this] val reqNoCharset = Request()
  reqNoCharset.contentType = "application/json"

  @Benchmark
  def charset_Utf8: Option[String] =
    reqUtf8.charset

  @Benchmark
  def charset_None: Option[String] =
    reqNoCharset.charset

}
