package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.concurrent.Scheduler
import com.twitter.finagle.exp.FinagleSchedulerService
import com.twitter.jvm.numProcs
import com.twitter.logging.Logger

/**
 * Service-loaded implementation used by `FinagleScheduler` to allow
 * users to enable the Fiber Scheduler using the flag
 * `com.twitter.finagle.exp.scheduler`.
 */
final class FiberSchedulerService extends FinagleSchedulerService {

  private[this] val log = Logger()

  override def paramsFormat =
    "<fiber>[:<fixed>:<num workers> | :<adaptive>[:<initial workers>:<min workers>:<max workers>]]"

  override def create(params: List[String]): Option[Scheduler] =
    params match {
      case "fiber" +: Nil =>
        create(defaultNumWorkers, true, defaultMinWorkers, defaultMaxWorkers)
      case "fiber" +: "fixed" +: Num(workers) +: Nil =>
        create(workers, false, 0, 0)
      case "fiber" +: "adaptive" +: Nil =>
        create(defaultNumWorkers, true, defaultMinWorkers, defaultMaxWorkers)
      case "fiber" +: "adaptive" +: Num(workers) +: Num(minWorkers) +: Num(maxWorkers) +: Nil =>
        create(workers, true, minWorkers, maxWorkers)
      case _ => None
    }

  private[this] def create(workers: Int, adaptive: Boolean, minWorkers: Int, maxWorkers: Int) = {
    val s = new FiberScheduler(
      workers,
      adaptive,
      minWorkers,
      maxWorkers,
      Config.Scheduling.maxQueuingDelay)
    if (Config.PendingTracking.enabled) {
      Some(FiberScheduler.withPendingTracking(s))
    } else {
      Some(s)
    }
  }

  private[this] def procs = numProcs().ceil.toInt
  private[this] def defaultNumWorkers = procs
  private[this] def defaultMinWorkers = procs / 3
  private[this] def defaultMaxWorkers = procs * 2

  private[this] object Num {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case _: java.lang.NumberFormatException => None
      }
    }
  }
}
