package com.twitter.finagle.util

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Try, Time, Duration}
import java.lang.reflect.Field
import java.util
import java.util.concurrent.TimeUnit
import org.jboss.netty.{util => netty}

private[finagle] object TimerStats {

  private val log = Logger.get()

  /**
   * Produces a [[com.twitter.finagle.stats.Stat]] that tracks how
   * much a task deviates from its expected time to be run.
   *
   * @param tickDuration the period at which the given `HashedWheelTimer` runs
   * @param statsReceiver typically scoped to "/finagle/timer/"
   */
  def deviation(
    hwt: netty.HashedWheelTimer,
    tickDuration: Duration,
    statsReceiver: StatsReceiver
  ): Unit = {
    val deviation = statsReceiver.stat("deviation_ms")

    // it is thread-safe to update this variable in the timer thread
    // because a `HashedWheelTimer` always uses a single thread to run
    // its tasks.
    var nextAtMillis =
      Time.now.inMilliseconds + tickDuration.inMilliseconds

    val timerTask = new netty.TimerTask {
      override def run(timeout: netty.Timeout): Unit = {
        val nowMillis = Time.now.inMilliseconds
        val deltaMillis = nowMillis - nextAtMillis
        nextAtMillis = nowMillis + tickDuration.inMilliseconds
        deviation.add(deltaMillis)
        hwt.newTimeout(this, tickDuration.inMilliseconds, TimeUnit.MILLISECONDS)
      }
    }
    hwt.newTimeout(timerTask, tickDuration.inMilliseconds, TimeUnit.MILLISECONDS)
  }

  /**
   * Produces a [[com.twitter.finagle.stats.Stat]] that tracks how
   * many tasks are pending to be run.
   *
   * @param nextRunAt when the task that computes the stat should be run next
   * @param statsReceiver typically scoped to "/finagle/timer/"
   */
  def hashedWheelTimerInternals(
    hwt: netty.HashedWheelTimer,
    nextRunAt: () => Duration,
    statsReceiver: StatsReceiver
  ): Unit = {
    val pendingTimeouts = statsReceiver.stat("pending_tasks")

    // this represents HashedWheelTimer's pending queue of tasks
    // that have yet to be scheduled into a bucket
    val queuedTimeouts: Try[util.Queue[_]] = Try {
      val timeoutsField = classOf[netty.HashedWheelTimer].getDeclaredField("timeouts")
      timeoutsField.setAccessible(true)
      timeoutsField.get(hwt).asInstanceOf[util.Queue[_]]
    }

    // an `Array[HashedWheelBucket]`, which is a private class.
    val buckets: Try[Array[Object]] = Try {
      val wheelField = classOf[netty.HashedWheelTimer].getDeclaredField("wheel")
      wheelField.setAccessible(true)
      wheelField.get(hwt).asInstanceOf[Array[Object]]
    }

    // the `Field` for `HashedWheelBucket.head`
    val bucketHeadField: Try[Field] =
      buckets.map { bs =>
        val headField = bs.head.getClass.getDeclaredField("head")
        headField.setAccessible(true)
        headField
      }

    def bucketTimeouts(hashedWheelBucket: Object): Int = {
      bucketHeadField.map { headField =>
        val head = headField.get(hashedWheelBucket) // this is a HashedWheelTimeout
        if (head == null) {
          0
        } else {
          val nextField = head.getClass.getDeclaredField("next")
          nextField.setAccessible(true)
          var num = 1 // count the one we've started with.
          var next = nextField.get(head)
          while (next != null) {
            num += 1
            next = nextField.get(next)
          }
          num
        }
      }.getOrElse(0)
    }

    def wheelTimeouts: Try[Int] =
      buckets.map { bs =>
        bs.map(bucketTimeouts).sum
      }

    val timerTask = new netty.TimerTask {
      override def run(timeout: netty.Timeout): Unit = {
        val startAt = System.nanoTime()
        for {
          qTimeouts <- queuedTimeouts
          wTimeouts <- wheelTimeouts
        } {
          pendingTimeouts.add(qTimeouts.size() + wTimeouts)
        }

        val elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startAt)
        if (log.isLoggable(Level.TRACE))
          log.trace(s"hashedWheelTimerInternals.run took $elapsedMicros us")
        hwt.newTimeout(this, nextRunAt().inMilliseconds, TimeUnit.MILLISECONDS)
      }
    }
    hwt.newTimeout(timerTask, nextRunAt().inMilliseconds, TimeUnit.MILLISECONDS)
  }
}
