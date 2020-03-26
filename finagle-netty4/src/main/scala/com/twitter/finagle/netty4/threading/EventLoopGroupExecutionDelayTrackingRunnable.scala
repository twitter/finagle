package com.twitter.finagle.netty4.threading

import com.twitter.finagle.stats.Stat
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Time}
import io.netty.util.concurrent.EventExecutor
import java.util.concurrent.{Callable, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

private[threading] class EventLoopGroupExecutionDelayTrackingRunnable(
  eventExecutor: EventExecutor,
  injectionPeriod: Duration,
  delayStat: Stat,
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
    if (eventExecutor.inEventLoop()) {
      Thread.currentThread()
    } else {
      eventExecutor
        .submit(new Callable[Thread] {
          override def call(): Thread = {
            Thread.currentThread()
          }
        }).await().get()
    }
  }

  private[this] val threadName: String = executorThread.getName
  private[this] var scheduledExecutionTime: Time = Time.now
  private[this] var watchTask: Option[ScheduledFuture[_]] = None

  setWatchTask()
  eventExecutor.scheduleWithFixedDelay(
    this,
    0,
    injectionPeriod.inMillis,
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
    scheduledExecutionTime = Time.now.plus(injectionPeriod)
    setWatchTask()
  }

  private[this] def setWatchTask(): Unit = {
    if (threadDumpEnabled) {
      watchTask = Some(
        dumpWatchThreadPool.get.schedule(
          new Runnable {
            override def run(): Unit = {
              var builder = new StringBuilder()
              builder
                .append(
                  s"THREAD: $threadName EXECUTION DELAY exceeded configured dump threshold. Thread stack trace:\n"
                )
              executorThread.getStackTrace.foreach { element => builder.append(s"    $element\n") }
              dumpLogger.warning(builder.toString())
            }
          },
          (injectionPeriod + threadDumpThreshold).inMillis,
          TimeUnit.MILLISECONDS
        )
      )
    }
  }
}
