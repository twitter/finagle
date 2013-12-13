package com.twitter.finagle.failters


import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Throw, Return, Future, Var}
import java.util.concurrent.RejectedExecutionException

/**
 * Randomly creates a Future.exception with the specified exception class before calling downstream
 * filters and services. Probability is in [0,1] where 0 means no failures.
 * @param probability
 * @param rejectWith A class to reject with. Must have a no-arguments constructor.
 * @param seed A random seed, otherwise a deterministic seed is used.
 */
case class RejectFailter[Req, Rep](probability: Var[Double],
                                   rejectWith: (() => Throwable) = (() => new RejectedExecutionException()),
                                   seed: Long = Failter.DefaultSeed,
                                   stats: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep] with Failter {


  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    if (prob == 0.0 || rand.nextDouble() >= prob) {
        passedStat.incr()
        service(req)
    } else {
        rejectedStat.incr()
        Future.exception(rejectWith())
    }
  }
}


/**
 * Like the reject filter, but will throw exceptions AFTER the service has been called.
 */
case class ByzantineRejectFailter[Req, Rep](probability: Var[Double],
                                            rejectWith: (() => Throwable) = (() => new RejectedExecutionException()),
                                            seed: Long = Failter.DefaultSeed,
                                            stats: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep] with Failter {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(req).transform { result =>
      if (prob == 0.0 || rand.nextDouble() >= prob) {
        passedStat.incr()
        Future.const(result)
      } else {
        rejectedStat.incr()
        // The result is now simply discarded
        Future.exception(rejectWith())
      }
    }
  }
}
