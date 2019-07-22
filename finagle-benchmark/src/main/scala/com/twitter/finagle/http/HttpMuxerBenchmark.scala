package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class HttpMuxerBenchmark extends StdBenchAnnotations {

  private[this] var i = 0

  private[this] val alreadyNormalized = IndexedSeq(
    "/foo/bar",
    "/cool/cool/cool",
    "/"
  )

  private[this] val excessiveSlashes = IndexedSeq(
    "/foo///bar",
    "/cool//cool////cool",
    "////"
  )

  private[this] val missingLeadingSlash = IndexedSeq(
    "foo/bar",
    "cool/cool/cool"
  )

  @Benchmark
  def normalize_alreadyNormalized: String = {
    i += 1
    if (i < 0) i = 0
    val path = alreadyNormalized(i % alreadyNormalized.size)
    HttpMuxer.normalize(path)
  }

  @Benchmark
  def normalize_excessiveSlashes: String = {
    i += 1
    if (i < 0) i = 0
    val path = excessiveSlashes(i % excessiveSlashes.size)
    HttpMuxer.normalize(path)
  }

  @Benchmark
  def normalize_missingLeadingSlash: String = {
    i += 1
    if (i < 0) i = 0
    val path = missingLeadingSlash(i % missingLeadingSlash.size)
    HttpMuxer.normalize(path)
  }

}
