package com.twitter.finagle.netty4.threading

import com.twitter.finagle.stats.Stat
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Duration
import com.twitter.util.Time
import io.netty.channel.SingleThreadEventLoop
import io.netty.util.concurrent.EventExecutor
import java.lang.management.ManagementFactory
import java.util.concurrent.Callable
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

private[threading] class EventLoopGroupTrackingRunnable(
  executor: EventExecutor,
  taskTrackingPeriod: Duration,
  delayStat: Stat,
  statsReceiver: StatsReceiver,
  threadDumpThreshold: Duration,
  dumpWatchThreadPool: Option[ScheduledThreadPoolExecutor],
  dumpLogger: Logger)
    extends Runnable {

  private[this] val threadDumpEnabled: Boolean = threadDumpThreshold.inMillis > 0

  // This code assumes that netty event executors have only one thread
  // and this allows us to pre-capture the thread, and dump only
  // the one thread in the executor. This is currently how netty is implemented
  // but this class will stop working if netty changes their implementation
  private[this] val executorThread: Thread = {
    if (executor.inEventLoop()) {
      Thread.currentThread()
    } else {
      executor
        .submit(new Callable[Thread] {
          override def call(): Thread = {
            Thread.currentThread()
          }
        }).await().get()
    }
  }

  private[this] val threadId = executorThread.getId
  private[this] val threadName: String = executorThread.getName

  private[this] var scheduledExecutionTime: Time = Time.now
  private[this] var watchTask: Option[ScheduledFuture[_]] = None

  private[this] val threadMXBean = ManagementFactory.getThreadMXBean

  private[this] val scopedStatsReceiver = statsReceiver.scope(threadName)
  private[this] val activeSocketsStat = scopedStatsReceiver.stat("active_sockets")
  private[this] val cpuTimeCounter = scopedStatsReceiver.counter("cpu_time_ms")

  // Accessed only from within the same netty thread
  private[this] var prevCPUTimeMs = 0L

  setWatchTask()
  executor.scheduleWithFixedDelay(
    this,
    0,
    taskTrackingPeriod.inMillis,
    java.util.concurrent.TimeUnit.MILLISECONDS
  )

  override def run(): Unit = {

    if (!watchTask.isEmpty) {
      watchTask.get.cancel(false)
    }

    val executionDelay = Time.now - scheduledExecutionTime
    if (threadDumpEnabled && executionDelay.inMillis > threadDumpThreshold.inMillis) {
      dumpLogger.warning(
        s"THREAD: $threadName EXECUTION DELAY is greater than ${threadDumpThreshold.inMillis}ms, was ${executionDelay.inMillis}ms"
      )
    }

    delayStat.add(executionDelay.inMillis)
    scheduledExecutionTime = Time.now.plus(taskTrackingPeriod)
    setWatchTask()

    var numActiveSockets = 0
    // This will be nio event loop or epoll event loop.
    executor.asInstanceOf[SingleThreadEventLoop].registeredChannelsIterator().forEachRemaining {
      channel =>
        if (channel.isActive) {
          numActiveSockets += 1
        }
    }
    activeSocketsStat.add(numActiveSockets)

    // `getThreadCPUTime` returns the time in nanoseconds.
    val currentCPUTimeMs = threadMXBean.getThreadCpuTime(threadId) / 1000000
    cpuTimeCounter.incr(currentCPUTimeMs - prevCPUTimeMs)
    prevCPUTimeMs = currentCPUTimeMs
  }

  private[this] def setWatchTask(): Unit = {
    if (threadDumpEnabled) {
      watchTask = Some(
        dumpWatchThreadPool.get.schedule(
          new Runnable {
            override def run(): Unit = {
              val builder = new StringBuilder()
              builder
                .append(
                  s"THREAD: $threadName EXECUTION DELAY exceeded configured dump threshold. Thread stack trace:\n"
                )
              executorThread.getStackTrace.foreach { element => builder.append(s"    $element\n") }
              dumpLogger.warning(builder.toString())
            }
          },
          (taskTrackingPeriod + threadDumpThreshold).inMillis,
          TimeUnit.MILLISECONDS
        )
      )
    }
  }
}
