package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.exp.routing.{
  HttpRouter,
  LinearPathRouter,
  Path,
  Schema,
  Segment,
  StringParam
}
import com.twitter.finagle.exp.routing.{Request => Req, Response => Rep, Route => ExpRoute}
import com.twitter.util.Future
import com.twitter.util.routing.Result
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

// to run:
// sbt 'project finagle-benchmark' 'jmh:run -prof gc HttpRouterBenchmark'
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
