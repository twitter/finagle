package com.twitter.finagle.netty4.threading

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Duration
import io.netty.channel.EventLoopGroup
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.reflect.internal.util.WeakHashSet

object EventLoopGroupTracker {

  private[threading] val trackedEventLoopGroups = new WeakHashSet[EventLoopGroup]()

  /**
   * Tracks execution delays in all the threads of a netty EventLoopGroup, by inserting runnables that
   * execute with a fixed delay of watchPeriod. Reports stats under workerpool/deviation_ms. Submitting
   * the same EventLoopGroup to this twice will cause the second submission to be ignored to avoid double
   * instrumentation.
   *
   * @param nettyEventLoopGroup The netty EventLoopGroup for which thread delays should be captured
   * @param trackingTaskPeriod The fixed delay for the runnables added to the EventLoopGroup threads to
   *                        capture thread tracking information.
   * @param dumpThreshold If > 0ms log seen delay for threads and the stack trace for threads at
   *                      the when the threads exceed the dumpThreshold delay.
   * @param statsReceiver The stats receiver under which execution delay stats should be reported.
   * @param dumpThreadPoolName The name of the thread pool to be created for dumping thread stacks.
   * @param logger The logger to be used to log thread dumps.
   */
  def track(
    nettyEventLoopGroup: EventLoopGroup,
    trackingTaskPeriod: Duration,
    dumpThreshold: Duration,
    statsReceiver: StatsReceiver,
    dumpThreadPoolName: String,
    logger: Logger
  ): Unit = synchronized {

    if (!trackedEventLoopGroups.contains(nettyEventLoopGroup)) {
      val threadDumpEnabled = dumpThreshold.inMillis > 0
      val dumpThresholdExceededThreadPool: Option[ScheduledThreadPoolExecutor] = {
        if (threadDumpEnabled) {
          Some(new ScheduledThreadPoolExecutor(2, new NamedPoolThreadFactory(dumpThreadPoolName)))
        } else {
          None
        }
      }

      val workerIter = nettyEventLoopGroup.iterator()
      val stat = statsReceiver.stat("workerpool", "deviation_ms")
      while (workerIter.hasNext) {
        val loop = workerIter.next()
        new EventLoopGroupTrackingRunnable(
          loop,
          trackingTaskPeriod,
          stat,
          statsReceiver,
          dumpThreshold,
          dumpThresholdExceededThreadPool,
          logger
        )
      }

      trackedEventLoopGroups.add(nettyEventLoopGroup)
    }
  }
}
