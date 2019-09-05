package com.twitter.finagle.exp

import com.twitter.app.GlobalFlag
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
        "<local> | <lifo> | <bridged>[:<num workers>] | <forkjoin>[:<num workers>]"
    )

private[finagle] object FinagleScheduler {
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
    scheduler().split(":").toList match {
      case "bridged" :: Integer(numWorkers) :: Nil => switchToBridged(numWorkers)
      case "bridged" :: Nil => switchToBridged(numProcs().ceil.toInt)

      case "forkjoin" :: Integer(numWorkers) :: Nil => switchToForkJoin(numWorkers)
      case "forkjoin" :: Nil => switchToForkJoin(numProcs().ceil.toInt)

      case "lifo" :: Nil =>
        log.info("Using LIFO local scheduler")
        Scheduler.setUnsafe(new LocalScheduler(true))

      case "local" :: Nil => // do nothing
      case _ =>
        throw new IllegalArgumentException("Wrong scheduler config: %s".format(scheduler()))
    }

    addGauges(Scheduler, DefaultStatsReceiver.scope("scheduler"), gauges)
  }
}
