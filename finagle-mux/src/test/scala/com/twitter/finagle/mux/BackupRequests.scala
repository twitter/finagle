package com.twitter.finagle.mux

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle.Filter
import com.twitter.finagle.Service
import com.twitter.finagle.client.BackupRequestFilter
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.MockWindowedPercentileHistogram
import com.twitter.util._
import com.twitter.util.tunable.Tunable
import java.util.concurrent.atomic.AtomicBoolean

object BackupRequests {

  def mkSlowService(slow: Promise[Response]): Service[Request, Response] =
    new Service[Request, Response] {
      val first = new AtomicBoolean(true)

      def apply(request: Request): Future[Response] =
        if (first.compareAndSet(true, false)) slow
        else Future.value(Response.empty)
    }

  def mkTestMuxBackupRequestFilter(
    timer: MockTimer
  ): Filter[Request, Response, Request, Response] = {
    val tunable: Tunable.Mutable[Double] = Tunable.mutable[Double]("tunable", 1.percent)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Return(_)) => ResponseClass.Success
    }
    val minSendBackupAfterMs: Int = 1
    val retryBudget = RetryBudget.Infinite
    val wp = new MockWindowedPercentileHistogram(timer)
    wp.add(1.second.inMillis.toInt)

    new BackupRequestFilter[Request, Response](
      tunable,
      true,
      minSendBackupAfterMs,
      classifier,
      (_, _) => retryBudget,
      retryBudget,
      Stopwatch.timeMillis,
      NullStatsReceiver,
      timer,
      () => wp,
      "client"
    )
  }

  def mkRequestWithBackup(client: Service[Request, Response])(ctl: TimeControl): Response = {
    val timer = new MockTimer()
    val brf = mkTestMuxBackupRequestFilter(timer)

    ctl.advance(3.seconds)
    timer.tick()

    val filtered = brf.andThen(client)
    val f = filtered(Request.empty)

    ctl.advance(2.seconds)
    timer.tick()

    Await.result(f, 5.seconds)
  }

}
