package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.http.filter.StatsFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Future}
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'jmh:run HttpStatsFilterBenchmark'
@State(Scope.Benchmark)
class HttpStatsFilterBenchmark extends StdBenchAnnotations {

  private[this] val request: Request = Request("/ping")

  private[this] val constResult: Future[Response] = Future.value {
    val rep = Response()
    rep.contentString = "pong"
    rep
  }

  private[this] val filter = new StatsFilter[Request](
    NullStatsReceiver
  )

  val statusCodes = Array(
    Status.Accepted,
    Status.BadRequest,
    Status.Continue,
    Status.EnhanceYourCalm,
    Status.Forbidden,
    Status.NoContent,
    Status.Ok,
    Status.InternalServerError
  )

  private[this] val constSvc = filter.andThen(Service.constant[Response](constResult))
  private[this] val randSvc = filter.andThen(Service.mk[Request, Response](_ =>
    Future.value {
      Response(statusCodes(Random.nextInt(statusCodes.size)))
    }))

  @Benchmark
  def constantStatusCode(): Response = {
    val res = constSvc(request)
    Await.result(res)
  }

  @Benchmark
  def randomStatusCode(): Response = {
    val res = randSvc(request)
    Await.result(res)
  }

}
