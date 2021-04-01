package com.twitter.finagle.offload

import com.twitter.conversions.DurationOps._
import com.twitter.app.Flaggable
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.logging.Logger
import com.twitter.util.{Duration, FuturePool}
import java.util.Locale

// An admission control mechanism that is uses the OffloadFilters work queue
// as it's source of knowledge as to whether things are backed up or not.
// See the `sample()` method for the theory of operation
private[finagle] object OffloadFilterAdmissionControl {

  private val log = Logger.get()

  sealed trait Params

  object Params {
    implicit val flaggable = new Flaggable[Params] {
      def parse(s: String): Params = s.toLowerCase(Locale.US) match {
        case "none" | "default" => Disabled
        case "enabled" => DefaultEnabledParams
        case other => parseParams(other)
      }

      // Expected format:
      // 'failurePercentile:windowSize'
      private[this] def parseParams(lowercaseFlag: String): Params = {
        import com.twitter.finagle.util.parsers._
        lowercaseFlag match {
          case list(duration(windowSize)) =>
            DefaultEnabledParams.copy(windowSize = windowSize)
          case unknown =>
            log.error(s"Unparsable OffloadFilterAdmissionControl value: $unknown")
            Disabled
        }
      }
    }
  }

  case class Enabled(
    windowSize: Duration,
    rejectionIncrement: Double,
    queueFullWaterMark: Long,
    failurePercentile: Double)
      extends Params
  case object Disabled extends Params

  // These parameters have been derived empirically with a little bit of intuition for garnish.
  //
  // - windowSize: The time width to monitor for compliance. A window of 500 to 1000 milliseconds
  //               seems to perform well a typical service.
  // - rejectionIncrement: derived empirically
  // - failurePercentile: derived theoretically and verified empirically. If we've been busy for
  //                      the whole window we consider ourselves backed up.
  // - queueFullWaterMark: derived from the hypothesis that if there is persistently work in the
  //                       queue we're backed up. Verified empirically.
  val DefaultEnabledParams: Enabled = Enabled(
    windowSize = 1000.millis,
    rejectionIncrement = 0.001,
    queueFullWaterMark = 1l,
    failurePercentile = 1.0,
  )

  def apply(futurePool: FuturePool, stats: StatsReceiver): Option[OffloadFilterAdmissionControl] = {
    admissionControl() match {
      case Disabled => None
      case e: Enabled => Some(instance(e, futurePool, stats))
    }
  }

  private[this] def instance(
    params: Enabled,
    futurePool: FuturePool,
    stats: StatsReceiver
  ): OffloadFilterAdmissionControl = {
    val ac = new OffloadFilterAdmissionControl(params, futurePool, stats, Rng.threadLocal)
    // Since this thread will never terminate we never want to wait for it.
    ac.setDaemon(true)
    ac.start()
    ac
  }

  // Note that this is not thread safe, nor is it intended to be.
  private final class MovingAverage(window: Int) {
    private[this] var sum: Long = 0
    private[this] var idx: Int = 0
    private[this] val history: Array[Int] = new Array[Int](window)

    def offer(value: Int): Double = {
      sum -= history(idx)
      sum += value
      history(idx) = value
      idx = (idx + 1) % window
      average
    }

    def average: Double = { sum.toDouble / window }

    override def toString: String = {
      val histStr = history.sum
      s"MovingAverage(sum: $sum, history: $histStr)"
    }
  }
}

private[finagle] final class OffloadFilterAdmissionControl(
  params: OffloadFilterAdmissionControl.Enabled,
  futurePool: FuturePool,
  stats: StatsReceiver,
  random: Rng)
    extends Thread("offload-ac-thread") {
  import OffloadFilterAdmissionControl._

  private val movingAvg = stats.addGauge("moving_average") { movingAverage.average.toFloat }
  private val rejectProb = stats.addGauge("rejection_probability") { rejectProbability.toFloat }

  // Some constants that were derived empirically
  private[this] final val ShortSleepTimeMs: Int = 1
  private[this] final val LongSleepTimeMs: Int = 5
  private[this] final val MaxRejectionFraction: Double = 1.0
  private[this] final val RecoveringScaleFactor: Double = 0.9

  // Note that these three are on notice: there is a good chance
  // that they don't need to be tuned.
  private[this] val failurePercentile = params.failurePercentile
  private[this] val rejectionIncrement = params.rejectionIncrement
  private[this] val queueFullWaterMark = params.queueFullWaterMark

  private[this] val movingAverage = new MovingAverage(
    params.windowSize.inMillis.toInt / ShortSleepTimeMs)

  // Since we're modifying this value from a single thread we don't need to
  // worry about synchronizing when accessing it.
  @volatile
  private[this] var rejectProbability: Double = 0.0

  override def run(): Unit = {
    try runLoop()
    finally {
      rejectProbability = 0.0
      log.info("Aborting loop and disabling AC.")
    }
  }

  private[this] def runLoop(): Unit = {
    while (true && !isInterrupted) {
      val nextSleep = sample()
      Thread.sleep(nextSleep)
    }
  }

  // returns the next sleep time in ms
  // public for testing purposes
  def sample(): Long = {
    // Theory of operation
    //
    // The idea is pretty simple: if there is persistently pending application work it is likely
    // that we are overloaded. When we get overloaded we should rapidly shed work until our queue
    // is healthy again and then rapidly transition to processing all requests.
    // This differs from other strategies that try to 'learn' what your max load is in that
    // it simply doesn't need to know what your maximum load is: if you're backed up you're backed
    // up and by using the work queue as a source of truth for this we can very rapidly detect
    // overload and react, and recover even faster.
    //
    // we use the number of tasks in the FuturePool to determine if we're overloaded.
    // One may ask, isn't latency more important than the number of tasks, and this is a really
    // good question. It's been observed that they are basically the same data, just related by
    // a scaling factor which is unique for each service. For example, if the offload queue is
    // found to be 0 then your queue latency will be a handful of microseconds, basically
    // the time it takes to put a task in the queue and wake a thread. However, if your queue size
    // is on average 100 and the average wait time for a task is 10us (and note that this number
    // bundles in the number of workers so with 10 threads one single task may actually be 100us of
    // work), your queue latency will be ~1ms. Since the data is otherwise the same other than a
    // scaling factor we take the number of tasks which is simple to query and has a satisfying
    // default value of 1 which means, if any work is waiting to be processed we may be backed up.
    val queueOverflow = futurePool.numPendingTasks >= queueFullWaterMark

    // The above talked a lot about averages and that is because execution is messy: we do see
    // bursts of tasks which will be processed very fast but none the less might cause unnecessary
    // rejection without some smoothing. We want to react fast, but not too fast. Therefore we take
    // a moving average. We offer either a 1 or a 0 for the same reason: we're
    // not so concerned as to how many tasks were pending, but whether they overflowed what we
    // considered 'busy'. This allows us to ask the sufficient question of 'where we busy over this
    // window' instead of getting the more complicated answer from the question of 'how busy were we
    // over this window'.
    val avg = movingAverage.offer(if (queueOverflow) 1 else 0)

    val oldProbability = rejectProbability
    if (avg < failurePercentile) {
      // This means over our window of time we don't consider ourselves backed up.
      // If we've gotten here we unconditionally consider the queue healthy and we
      // currently have capacity to process requests.
      rejectProbability = 0.0
    } else if (!queueOverflow) {
      // It looks like we may be in a recovery phase. Start to rapidly step down
      // our rejection fraction in an geometric decay fashion.
      rejectProbability = oldProbability * RecoveringScaleFactor
    } else {
      // We have queued tasks and have had them for a while. Time to reject some
      // more work until we can recover from this overload scenario.
      rejectProbability = math.min(MaxRejectionFraction, oldProbability + rejectionIncrement)
    }

    // Now choose how long we'll wait until probing again. We should probe
    // rapidly under any of the following conditions:
    // * We were rejecting the cycle before this
    // * We consider our current queue size overloaded
    // * We have a history of being overloaded
    if (queueOverflow || oldProbability > 0.0 || avg >= failurePercentile) ShortSleepTimeMs
    else LongSleepTimeMs
  }

  def shouldReject: Boolean = {
    // Only read the volatile once
    val prob = rejectProbability
    0.0 < prob && random.nextDouble() <= prob
  }
}
