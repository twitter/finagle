package com.twitter.finagle.exp

import com.twitter.app.{GlobalFlag, LoadService}
import com.twitter.concurrent.{BridgedThreadPoolScheduler, LocalScheduler, Scheduler}
import com.twitter.finagle.stats.{DefaultStatsReceiver, Gauge, StatsReceiver}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.jvm.numProcs
import java.util.concurrent.{LinkedTransferQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable

object scheduler
    extends GlobalFlag[String](
      "local",
      "Which scheduler to use for futures " +
        "<local> | <lifo> | <bridged>[:<num workers>] | <forkjoin>[:<num workers>] | " +
        FinagleSchedulerService.all.map(_.paramsFormat).mkString(" | ")
    )

trait FinagleSchedulerService {
  def paramsFormat: String
  def create(params: List[String]): Option[Scheduler]
}

object FinagleSchedulerService {
  private[exp] lazy val all = LoadService[FinagleSchedulerService]
}

private[finagle] class BaseFinagleScheduler(
  config: String,
  loadServices: () => Seq[FinagleSchedulerService]) {
  private val log = DefaultLogger

  private[this] val gauges: mutable.Buffer[Gauge] = mutable.ArrayBuffer[Gauge]()

  private object Integer {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case _: java.lang.NumberFormatException => None
      }
    }
  }

  private def switchToBridged(numWorkers: Int): Unit = {
    val queue = new LinkedTransferQueue[Runnable]()

    Scheduler.setUnsafe(
      new BridgedThreadPoolScheduler(
        "bridged scheduler",
        (threadFactory: ThreadFactory) =>
          new ThreadPoolExecutor(
            numWorkers,
            numWorkers,
            0L,
            TimeUnit.MILLISECONDS,
            queue,
            threadFactory
          )
      )
    )

    log.info("Using bridged scheduler with %d workers".format(numWorkers))
  }

  private def switchToForkJoin(numWorkers: Int): Unit = {
    log.info("Using forkjoin scheduler with %d workers".format(numWorkers))
    Scheduler.setUnsafe(new ForkJoinScheduler(numWorkers, DefaultStatsReceiver.scope("forkjoin")))
  }

  // exposed for testing
  private[exp] def addGauges(
    scheduler: Scheduler,
    statsReceiver: StatsReceiver,
    gauges: mutable.Buffer[Gauge]
  ): Unit = {
    gauges.synchronized {
      gauges += statsReceiver.addGauge("dispatches") {
        scheduler.numDispatches.toFloat
      }
      gauges += statsReceiver.addGauge("blocking_ms") {
        TimeUnit.NANOSECONDS.toMillis(scheduler.blockingTimeNanos)
      }
    }
  }

  def init(): Unit = {
    config.split(":").toList match {
      case "bridged" :: Integer(numWorkers) :: Nil => switchToBridged(numWorkers)
      case "bridged" :: Nil => switchToBridged(numProcs().ceil.toInt)

      case "forkjoin" :: Integer(numWorkers) :: Nil => switchToForkJoin(numWorkers)
      case "forkjoin" :: Nil => switchToForkJoin(numProcs().ceil.toInt)

      case "lifo" :: Nil =>
        log.info("Using LIFO local scheduler")
        Scheduler.setUnsafe(new LocalScheduler(true))

      case "local" :: Nil => // do nothing

      case params =>
        loadServices().flatMap(_.create(params)) match {
          case Seq() =>
            throw new IllegalArgumentException("Wrong scheduler config: %s".format(scheduler()))
          case Seq(scheduler) =>
            log.info(s"Using service loaded scheduler $scheduler")
            Scheduler.setUnsafe(scheduler)
          case multiple =>
            throw new IllegalArgumentException(
              s"Multiple service loaded schedulers found: ${multiple.mkString(", ")}")
        }
    }

    addGauges(Scheduler, DefaultStatsReceiver.scope("scheduler"), gauges)
  }
}

private[finagle] object FinagleScheduler
    extends BaseFinagleScheduler(scheduler(), () => FinagleSchedulerService.all)
