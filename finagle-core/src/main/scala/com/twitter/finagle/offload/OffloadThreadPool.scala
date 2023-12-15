package com.twitter.finagle.offload

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.LoadService
import com.twitter.logging.Logger
import java.util.concurrent.ExecutorService

private object OffloadThreadPool {

  private[this] val logger = Logger.get()

  /** Construct an `ExecutorService` with the proper thread names and metrics */
  def apply(poolSize: Int, maxQueueLen: Int, stats: StatsReceiver): ExecutorService = {
    LoadService[OffloadThreadPoolFactory]() match {
      case Seq() =>
        logger.info("Constructing the default OffloadThreadPool executor service")
        new DefaultThreadPoolExecutor(
          poolSize = poolSize,
          maxQueueLen = maxQueueLen,
          stats = stats
        )

      case Seq(factory) =>
        logger.info(s"Constructing OffloadThreadPool using $factory")
        factory.newPool(poolSize, maxQueueLen, stats)

      case multiple =>
        logger.error(
          s"Found multiple `OffloadThreadPoolFactory`s: $multiple. " +
            s"Using the default implementation.")
        new DefaultThreadPoolExecutor(
          poolSize = poolSize,
          maxQueueLen = maxQueueLen,
          stats = stats
        )
    }
  }
}
