package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.exp.routing.HttpRouter
import com.twitter.finagle.http.exp.routing.LinearPathRouter
import com.twitter.finagle.http.exp.routing.Path
import com.twitter.finagle.http.exp.routing.Schema
import com.twitter.finagle.http.exp.routing.Segment
import com.twitter.finagle.http.exp.routing.StringParam
import com.twitter.finagle.exp.routing.{Request => Req}
import com.twitter.finagle.exp.routing.{Response => Rep}
import com.twitter.finagle.exp.routing.{Route => ExpRoute}
import com.twitter.util.Future
import com.twitter.util.routing.Result
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

// to run:
// /bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'HttpRouterBenchmark'
@State(Scope.Benchmark)
class HttpRouterBenchmark extends StdBenchAnnotations {

  private[this] val svc = Service.mk[Req[Request], Rep[Response]] { _ =>
    Future.value(Rep(Response()))
  }

  private[this] val router = HttpRouter
    .newBuilder(LinearPathRouter.Generator)
    .withLabel("benchmark")
    .withRoute(
      ExpRoute(
        service = svc,
        label = "slash",
        schema = Schema(method = Method.Get, path = Path(Seq(Segment.Slash)))))
    .withRoute(ExpRoute(
      service = svc,
      label = "what_up",
      schema = Schema(
        method = Method.Get,
        path =
          Path(Seq(Segment.Slash, Segment.Constant("what"), Segment.Slash, Segment.Constant("up"))))
    ))
    .withRoute(ExpRoute(
      service = svc,
      label = "its_cool",
      schema = Schema(
        method = Method.Get,
        path = Path(
          Seq(Segment.Slash, Segment.Constant("its"), Segment.Slash, Segment.Constant("cool"))))
    ))
    .withRoute(ExpRoute(
      service = svc,
      label = "yo_dynamic",
      schema = Schema(
        method = Method.Get,
        path = Path(
          Seq(
            Segment.Slash,
            Segment.Constant("yo"),
            Segment.Slash,
            Segment.Parameterized(StringParam("name")))))
    ))
    .newRouter()

  private[this] var i = 0

  private[this] val routerRequests = IndexedSeq(
    Request("/what/up"),
    Request("/its/cool")
  )

  private[this] val parameterizedRequests = IndexedSeq(
    Request("/yo/abc"),
    Request("/yo/sssssuuupppppp"),
    Request("/yo/"),
    Request("/yo"),
    Request("/yo/12345"),
    Request("/yo/12345/678910")
  )

  private[this] val excessiveSlashes = IndexedSeq(
    Request("/what///up"),
    Request("////its////cool//"),
    Request("////")
  )

  @Benchmark
  def route_exactMatch: Result = {
    i += 1
    if (i < 0) i = 0
    val request = routerRequests(i % routerRequests.size)
    router(Req(request))
  }

  @Benchmark
  def route_parameterizedMatch: Result = {
    i += 1
    if (i < 0) i = 0
    val request = parameterizedRequests(i % parameterizedRequests.size)
    router(Req(request))
  }

  @Benchmark
  def route_excessiveSlashes: Result = {
    i += 1
    if (i < 0) i = 0
    val request = excessiveSlashes(i % excessiveSlashes.size)
    router(Req(request))
  }

}
