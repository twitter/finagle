package com.twitter.finagle.offload

import com.twitter.conversions.DurationOps._
import com.twitter.app.Flaggable
import com.twitter.finagle.{Failure, Filter, Service, SimpleFilter, Stack}
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, FuturePool}
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

// An admission control mechanism that is uses the OffloadFilters work queue
// as it's source of knowledge as to whether things are backed up or not.
// See the `sample()` method for the theory of operation
private[finagle] object OffloadFilterAdmissionControl {

  private[this] final class AcFilter(ac: OffloadFilterAdmissionControl, stats: StatsReceiver)
      extends Filter.TypeAgnostic {

    private val rejections = stats.counter("rejections")

    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
      // Save a local reference so we don't need to pointer chase as much.
      private val ac = AcFilter.this.ac
      def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
        if (!ac.shouldReject) service(request)
        else {
          rejections.incr()
          Failure.FutureRetryableNackFailure
        }
      }
    }
  }

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
      // 'failurePercentile:maxQueueDelay'
      private[this] def parseParams(lowercaseFlag: String): Params = {
        import com.twitter.finagle.util.parsers._
        lowercaseFlag match {
          case list(duration(maxQueueDelay)) =>
            DefaultEnabledParams.copy(maxQueueDelay = maxQueueDelay)
          case unknown =>
            log.error(s"Unparsable OffloadFilterAdmissionControl value: $unknown")
            Disabled
        }
      }
    }
  }

  case class Enabled(maxQueueDelay: Duration) extends Params
  case object Disabled extends Params

  // These parameters have been derived empirically with a little bit of intuition for garnish.
  // - maxQueueDelay: The acceptable delay for tasks in the worker pool before we start to reject work.
  val DefaultEnabledParams: Enabled = Enabled(maxQueueDelay = 20.milliseconds)

  /**
   * This is where we inject admission control, if necessary. We only
   * inject admission control if it is generally enabled and we have
   * the global `FuturePool` with the admission controller available.
   */
  def maybeInjectAC(pool: FuturePool, params: Stack.Params): Stack.Params = {
    val acEnabled = params[ServerAdmissionControl.Param].serverAdmissionControlEnabled
    pool match {
      case p: OffloadFuturePool if acEnabled && p.admissionControl.isDefined =>
        val stats = params[Stats].statsReceiver.scope("admission_control", "offload_based")
        params + ServerAdmissionControl.Filters(
          Some(Seq(_ => new AcFilter(p.admissionControl.get, stats))))

      case _ => params
    }
  }

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
    val ac = new OffloadFilterAdmissionControl(params, futurePool, stats)
    // Since this thread will never terminate we never want to wait for it.
    ac.setDaemon(true)
    ac.start()
    ac
  }
}

private[finagle] final class OffloadFilterAdmissionControl(
  params: OffloadFilterAdmissionControl.Enabled,
  futurePool: FuturePool,
  stats: StatsReceiver)
    extends Thread("offload-ac-thread") {
  import OffloadFilterAdmissionControl._

  private[this] val projectedDelayMs = stats.stat("projected_delay_ms")

  // Some constants that were derived empirically
  private[this] final val ShortSleepTimeMs: Int = 1
  private[this] final val LongSleepTimeMs: Int = 5

  private[this] val outstandingDecTasks = new AtomicInteger()
  private[this] val decTask = () => { outstandingDecTasks.decrementAndGet(); () }
  private[this] val maxDelayCount: Int =
    math.max(1, math.round(params.maxQueueDelay.inMillis.toDouble / ShortSleepTimeMs).toInt)

  override def run(): Unit = {
    try runLoop()
    finally {
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
    // If we have tasks in the queue they may take some time to process. We see what
    // the current delay is by adding tasks at the predefined interval (1ms) and just
    // counting how many are outstanding using an atomic integer. This means that 20
    // outstanding tasks says that there is roughly a 20ms delay for elements to get
    // through the queue. For reference, an empty queue should take on the order of
    // 10 to 20 microseconds to process this task.
    if (futurePool.numPendingTasks > 0) {
      val last = outstandingDecTasks.getAndIncrement()
      projectedDelayMs.add(last * ShortSleepTimeMs)
      futurePool(decTask())
      ShortSleepTimeMs
    } else {
      LongSleepTimeMs
    }
  }

  def shouldReject: Boolean = outstandingDecTasks.get > maxDelayCount
}
