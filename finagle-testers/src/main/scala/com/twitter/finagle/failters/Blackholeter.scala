package com.twitter.finagle.failters

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Return, Throw, Future, Var}

/**
 * A blackholeter rejects/fails by returning a promise from a service which will never be fulfilled.
 *
 * This is useful for testing timeout response. Note that the underlying service is NOT called in this variant.
 *
 * @param probability [0,1], where 1 is 100% blackhole mode
 */
case class Blackholeter[Req, Rep](probability: Var[Double],
                                  seed: Long = Failter.DefaultSeed,
                                  stats: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep] with Failter  {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    if (prob == 0.0 || rand.nextDouble() >= prob) {
        passedStat.incr()
        service(req)
    } else {
        rejectedStat.incr()
        Future.never
    }
  }
}


/**
 * A blackholeter rejects/fails by returning a promise from a service which will never be fulfilled.
 *
 * This is useful for testing timeout response. Note that the underlying service IS called in this variant,
 * but the result is DISCARDED. Useful for checking for idempotent behavior.
 *
 * @param probability [0,1] whre 1 is 100% blackhole mode
 */
case class ByzantineBlackholeter[Req, Rep](probability: Var[Double],
                                  seed: Long = Failter.DefaultSeed,
                                  stats: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep] with Failter  {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(req).transform { result =>
      if (prob == 0.0 || rand.nextDouble() >= prob) {
        passedStat.incr()
        Future.const(result)
      } else {
        rejectedStat.incr()
        // The result is now simply discarded
        Future.never
      }
    }
  }
}
