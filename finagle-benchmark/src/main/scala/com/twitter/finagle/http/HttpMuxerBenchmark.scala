package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Future
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class HttpMuxerBenchmark extends StdBenchAnnotations {

  private[this] val svc = Service.mk { _: Request => Future.value(Response()) }
  private[this] val muxerRequests = IndexedSeq(
    Request("/what/up"),
    Request("/its/cool")
  )
  private[this] val routes = IndexedSeq(
    Route("/", svc),
    Route("/whats/up", svc),
    Route("/its/cool", svc),
    Route("/yo/", svc)
  )

  private[this] val muxer = new HttpMuxer(routes)

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

  private[this] val parameterizedRequests = IndexedSeq(
    Request("/yo/abc"),
    Request("/yo/sssssuuupppppp"),
    Request("/yo/"),
    Request("/yo"),
    Request("/yo/12345"),
    Request("/yo/12345/678910")
  )

  private[this] val excessiveSlashesRequests = excessiveSlashes.map(Request(_))

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

  @Benchmark
  def route_exactMatch: Option[Route] = {
    i += 1
    if (i < 0) i = 0
    val request = muxerRequests(i % muxerRequests.size)
    muxer.route(request)
  }

  @Benchmark
  def route_excessiveSlashes: Option[Route] = {
    i += 1
    if (i < 0) i = 0
    val req = excessiveSlashesRequests(i % excessiveSlashesRequests.size)
    muxer.route(req)
  }

  @Benchmark
  def route_parameterizedMatch: Option[Route] = {
    i += 1
    if (i < 0) i = 0
    val request = parameterizedRequests(i % parameterizedRequests.size)
    muxer.route(request)
  }

}
