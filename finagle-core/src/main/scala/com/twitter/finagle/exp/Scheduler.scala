package com.twitter.finagle.exp

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.{BridgedThreadPoolScheduler, Scheduler}
import com.twitter.jvm.numProcs
import java.util.concurrent.{BlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.logging.{Level, Logger}
import com.twitter.finagle.stats.DefaultStatsReceiver

object scheduler extends GlobalFlag(
  "local",
  "Which scheduler to use for futures "+
  "<local> | <bridged>[:<num workers>] | <forkjoin>[:<num workers>]"
)

private[finagle] object FinagleScheduler {
  private val log = Logger.getLogger("finagle")

  private object Integer {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case _ => None
      }
    }
  }

  private def switchToBridged(numWorkers: Int) {
    val queue = try
      Class.forName(
        "java.util.concurrent.LinkedTransferQueue"
      ).newInstance.asInstanceOf[BlockingQueue[Runnable]]
    catch {
      case _: ClassNotFoundException => {
        log.info("bridged scheduler is not available on pre java 7, using local instead")
        return
      }
    }

    Scheduler.setUnsafe(new BridgedThreadPoolScheduler(
      "bridged scheduler",
      (threadFactory: ThreadFactory) => new ThreadPoolExecutor(
        numWorkers,
        numWorkers,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        threadFactory)))

    log.info("Using bridged scheduler with %d workers".format(numWorkers))
  }
  
  private def switchToForkJoin(numWorkers: Int) {
    log.info("Using forkjoin scheduler with %d workers".format(numWorkers))
    Scheduler.setUnsafe(
      new ForkJoinScheduler(numWorkers, DefaultStatsReceiver.scope("forkjoin")))
  }

  def init() {
    scheduler().split(":").toList match {
      case "bridged" :: Integer(numWorkers) :: Nil => switchToBridged(numWorkers)
      case "bridged" :: Nil => switchToBridged(numProcs().ceil.toInt)

      case "forkjoin" :: Integer(numWorkers) :: Nil => switchToForkJoin(numWorkers)
      case "forkjoin" :: Nil => switchToForkJoin(numProcs().ceil.toInt)

      case "local" :: Nil => // do nothing
      case _ =>
        throw new IllegalArgumentException("Wrong scheduler config: %s".format(scheduler()))
    }
  }
}
