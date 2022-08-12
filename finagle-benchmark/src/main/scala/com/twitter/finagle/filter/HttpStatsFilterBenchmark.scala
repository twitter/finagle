package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Status
import com.twitter.finagle.http.filter.StatsFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Future
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import scala.util.Random

// /bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'HttpStatsFilterBenchmark'
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
