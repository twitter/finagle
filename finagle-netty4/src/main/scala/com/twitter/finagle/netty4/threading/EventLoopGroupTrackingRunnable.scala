package com.twitter.finagle.netty4.threading

import com.twitter.finagle.stats.HistogramFormat
import com.twitter.finagle.stats.MetricBuilder
import com.twitter.finagle.stats.MetricBuilder.HistogramType
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
  private[this] val pendingTasksStat = scopedStatsReceiver.stat(
    MetricBuilder(
      metricType = HistogramType,
      name = Seq("pending_tasks"),
      percentiles = Array[Double](0.25, 0.50, 0.75, 0.90, 0.95, 0.99),
      histogramFormat = HistogramFormat.FullSummary
    )
  )
  private[this] val allSocketsStat = scopedStatsReceiver.stat(
    MetricBuilder(
      metricType = HistogramType,
      name = Seq("all_sockets"),
      percentiles = Array[Double](0.50),
      histogramFormat = HistogramFormat.FullSummary
    )
  )
  private[this] val cpuTimeCounter = scopedStatsReceiver.counter("cpu_time_ms")
  private[this] val cpuUtilStat = scopedStatsReceiver.stat(
    MetricBuilder(
      metricType = HistogramType,
      name = Seq("cpu_util"),
      percentiles = Array[Double](0.25, 0.50, 0.75, 0.90, 0.95, 0.99),
      histogramFormat = HistogramFormat.FullSummary
    )
  )

  // Accessed only from within the same netty thread
  private[this] var prevCPUTimeNs = 0L
  private[this] var prevWallTimeNs = 0L

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

    val now = Time.now
    val executionDelay = now - scheduledExecutionTime
    if (threadDumpEnabled && executionDelay.inMillis > threadDumpThreshold.inMillis) {
      dumpLogger.warning(
        s"THREAD: $threadName EXECUTION DELAY is greater than ${threadDumpThreshold.inMillis}ms, was ${executionDelay.inMillis}ms"
      )
    }

    delayStat.add(executionDelay.inMillis)
    scheduledExecutionTime = now.plus(taskTrackingPeriod)
    setWatchTask()

    // This will be nio event loop or epoll event loop.
    val loop = executor.asInstanceOf[SingleThreadEventLoop]
    allSocketsStat.add(loop.registeredChannels())
    pendingTasksStat.add(loop.pendingTasks())

    // `getThreadCPUTime` returns the time in nanoseconds.
    val currCPUTimeNs = threadMXBean.getThreadCpuTime(threadId)
    val cpuTime = currCPUTimeNs - prevCPUTimeNs

    val currWallTimeNs = System.nanoTime()
    val wallTimeNs = currWallTimeNs - prevWallTimeNs
    cpuTimeCounter.incr(TimeUnit.NANOSECONDS.toMillis(cpuTime))
    if (prevWallTimeNs != 0 && wallTimeNs != 0) {
      cpuUtilStat.add(
        10000 * cpuTime / wallTimeNs
      )
    }
    prevCPUTimeNs = currCPUTimeNs
    prevWallTimeNs = currWallTimeNs
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
