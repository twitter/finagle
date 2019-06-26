package com.twitter.finagle.server

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.{Counter, Gauge, StatsReceiver, Verbosity}
import com.twitter.finagle.{
  Filter,
  Service,
  ServiceFactory,
  ServiceFactoryProxy,
  Stack,
  Stackable,
  param
}
import com.twitter.util.{Closable, Future, Time, Timer}
import java.lang.{Boolean => JBoolean}
import java.util.Collections
import java.util.concurrent.atomic.LongAdder
import java.{util => ju}
import scala.collection.mutable

/**
 * Track per-thread request utilization which aids in identifying
 * servers with request and/or connection imbalance.
 *
 * @param statsReceiver normally will be scoped to "srv/$server_name/thread_usage/requests"
 */
private class ThreadUsage(statsReceiver: StatsReceiver, timer: Timer) extends Closable { self =>

  // all thread's current counts.
  //
  // Uses weak refs to the counters to avoid a memory leak when threads are not long-lived.
  private val allCounts: ju.Set[LongAdder] = {
    val cache: Cache[LongAdder, JBoolean] = Caffeine
      .newBuilder()
      .weakKeys()
      .build[LongAdder, JBoolean]()
    Collections.newSetFromMap[LongAdder](cache.asMap())
  }

  private[this] val perThreadCounter = new ThreadLocal[Counter] {
    override def initialValue(): Counter =
      statsReceiver
        .scope("per_thread")
        .counter(Verbosity.Debug, Thread.currentThread.getName)
  }

  private[this] val aggregateCounter = new ThreadLocal[LongAdder] {
    override def initialValue(): LongAdder = {
      val value = new LongAdder()
      allCounts.add(value)
      value
    }
  }

  @volatile
  private[this] var perThreadMean, perThreadStddev, relativeStddev = 0.0f

  private[this] val gauges: Seq[Gauge] = Seq(
    statsReceiver.addGauge(Verbosity.Debug, "mean") { perThreadMean },
    statsReceiver.addGauge(Verbosity.Debug, "stddev") { perThreadStddev },
    statsReceiver.addGauge("relative_stddev") { relativeStddev }
  )

  private[this] def compute(): Unit = {
    // Notes:
    // - we could compute standard deviation in a single pass along with `total`.
    //   however, the expectation is the number of threads is relatively small so
    //   this should not be a big deal in practice.
    //
    // - while iterating, we also clear the counts for the next time this runs.
    var sum = 0L
    val counts = new mutable.ArrayBuffer[Long](allCounts.size)
    val iter = allCounts.iterator
    while (iter.hasNext) {
      val value = iter.next().sumThenReset()
      if (value != 0) {
        sum += value
        counts += value
      }
    }
    val n = counts.size

    val mean = if (n == 0) 0.0f else sum.toDouble / n.toDouble
    perThreadStddev =
      if (n <= 1) 0.0f
      else {
        var sumOfSquaredDeltas = 0.0
        val iter = counts.iterator
        while (iter.hasNext) {
          val deltaFromMean = iter.next().toDouble - mean
          sumOfSquaredDeltas += deltaFromMean * deltaFromMean
        }
        math.sqrt(sumOfSquaredDeltas / n).toFloat
      }
    perThreadMean = mean.toFloat

    // See https://en.wikipedia.org/wiki/Coefficient_of_variation
    relativeStddev = if (mean == 0.0f) 0.0f else (perThreadStddev / mean).toFloat
  }

  private[this] val computeTask = timer.schedule(1.minute) {
    compute()
  }

  /**
   * Called when the current thread is handling a request.
   */
  def increment(): Unit = {
    aggregateCounter.get.increment()
    perThreadCounter.get.incr()
  }

  def close(deadline: Time): Future[Unit] = {
    computeTask.cancel()
    gauges.foreach(_.remove())
    Future.Done
  }
}

object ThreadUsage {

  val role: Stack.Role = Stack.Role("ThreadUsage")

  private class ThreadUsageFilter[Req, Rep](threadUsage: ThreadUsage)
      extends Filter[Req, Rep, Req, Rep] {

    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
      threadUsage.increment()
      service(request)
    }
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = {
    new Stack.Module2[param.Stats, param.Timer, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = ThreadUsage.role
      val description: String = "Per-thread request handling metrics"
      def make(
        stats: param.Stats,
        timer: param.Timer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val sr = stats.statsReceiver
        if (sr.isNull) next
        else {
          val usage = new ThreadUsage(sr.scope("thread_usage", "requests"), timer.timer)
          val filter = new ThreadUsageFilter[Req, Rep](usage)
          new ServiceFactoryProxy(filter.andThen(next)) {
            override def close(deadline: Time): Future[Unit] = {
              usage.close(deadline).before {
                self.close(deadline)
              }
            }
          }
        }
      }
    }
  }
}
