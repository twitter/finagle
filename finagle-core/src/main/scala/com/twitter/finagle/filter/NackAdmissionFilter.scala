package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.{Counter, Gauge, StatsReceiver, Verbosity}
import com.twitter.finagle.util.{Ema, Rng}
import com.twitter.util._

private[finagle] object NackAdmissionFilter {
  private val OverloadFailure = Future.exception(
    Failure("Request not issued to the backend due to observed overload.",
      Failure.Rejected | Failure.NonRetryable)
  )
  val role: Stack.Role = new Stack.Role("NackAdmissionFilter")

  /**
   * For feature roll out only.
   */
  private val EnableNackAcToggle = CoreToggles(
    "com.twitter.finagle.core.UseClientNackAdmissionFilter"
  )
  private def enableNackAc(): Boolean = EnableNackAcToggle(ServerInfo().id.hashCode)

  /**
   * An upper bound on what percentage of requests this filter will drop.
   * Without this, it is possible for the filter to fail closed as the EMA
   * approaches zero. If no requests are allowed through, no new information is
   * gathered about the service, preventing recovery.
   */
  private[finagle] val MaxDropProbability: Double = 0.75

  /**
   * By default, the EMA window is 2 minutes: any response that the filter
   * receives over a 2 minute rolling window affects the EMA's value. In other
   * words, the EMA "forgets" history older than 2 minutes.
   *
   * E.g., if the server nacks every request received for one minute, then
   * two minutes pass without the server receiving any requests, and then
   * the server receives a small number of non-nacks ("accepts"), the EMA will
   * end up being very close to 1.
   */
  private val DefaultWindow: Duration = 2.minutes

  /**
   * By default, the client will send all requests when the accept rate EMA is
   * between 50% and 100%. If the EMA drops below 50%, the filter will drop any
   * given request with probability proportional to the EMA.
   *
   * E.g., if the EMA is 20%, the filter will drop any given request with
   * 100 - (2 * 20) = 60% probability. If the EMA is 10%, the filter will drop
   * any given request with 100 - (2 * 10) = 80% probability.
   */
  private val DefaultNackRateThreshold: Double = 0.5

  /**
   * If the request rate is below `rpsThreshold`, the filter will not lower
   * the ema value or drop requests. If the request rate is equal or greater,
   * the filter will take effect.
   *
   * Note: the value of this threshold was empirically found to be effective.
   */
  private val rpsThreshold: Long = 5


  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  private[finagle] object Param {
    /**
     * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
     * [[com.twitter.finagle.filter.NackAdmissionFilter]] module.
     */
    case class Configured(window: Duration, nackRateThreshold: Double) extends Param

    /**
     * Disables this role
     */
    case object Disabled extends Param

    implicit val param: Stack.Param[NackAdmissionFilter.Param] =
      Stack.Param(Configured(DefaultWindow, DefaultNackRateThreshold))
  }

  private[finagle] val Disabled: Param = Param.Disabled

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[NackAdmissionFilter.Param, param.Stats, ServiceFactory[Req, Rep]] {
      val description: String = "Probabilistically drops requests to the underlying service."
      val role: Stack.Role = NackAdmissionFilter.role

      def make(
        _param: Param,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] =  _param match {
        case Param.Configured(window, threshold) =>
          val param.Stats(stats) = _stats

          // Create the filter with the given window and nack rate threshold.
          val filter = new NackAdmissionFilter[Req, Rep](
            window,
            threshold,
            Rng.threadLocal,
            stats.scope("nack_admission_control")
          )

          // This creates a filter that is shared across all endpoints, which is
          // required for proper operation.
          filter.andThen(next)

        case Param.Disabled =>
          next
      }
    }
}

/**
 * This filter probabilistically drops requests if the nack rate exceeds the
 * `nackRateThreshold`. In the case that most or all of the cluster which the
 * client is speaking to is overloaded, this will help the cluster cool off.
 *
 * The implementation of this filter is heavily inspired by Chapter 21, section
 * "Client-Side Throttling" of O'Reilly's "Site Reliability Engineering: How
 * Google Runs Production Systems", by Beyer, Jones, Petoff, and Murphy, 1e.
 *
 * @param window Size of moving window for exponential moving average, which is
 * used to keep track of the ratio of nacked responses to accepted responses
 * and compute the client's accept rate. E.g., if set to 1 second, then only
 * requests occurring over the previous second will be used to calculate the
 * EMA. The window size influences how the EMA behaves in roughly the following
 * ways:
 *
 * - an EMA with a shorter window duration will respond more quickly to changes
 * in cluster performance, but will keep a less accurate estimate of the
 * long-term average accept rate;
 *
 * - an EMA with a longer window duration will respond more slowly to changes
 * in cluster performance, but will keep a more accurate estimate of the long-
 * term average accept rate.
 *
 * @param nackRateThreshold Constant which determines how aggressively the
 * filter drops requests. For example, if set to 1/2, then the highest nack
 * rate the filter will tolerate before probabilistically dropping requests is
 * 50%; if set to 1/3, then the highest nack rate tolerated is 33.3%. In
 * general, if set to x, the highest nack rate tolerated is x.
 *
 * @param random Random number generator used in probability calculation.
 */
private[finagle] class NackAdmissionFilter[Req, Rep](
  window: Duration,
  nackRateThreshold: Double,
  random: Rng,
  statsReceiver: StatsReceiver,
  monoTime: Ema.Monotime = new Ema.Monotime
) extends SimpleFilter[Req, Rep] {
  import NackAdmissionFilter._

  require(window > Duration.Zero, s"window size must be positive: $window")
  require(nackRateThreshold < 1, s"nackRateThreshold must lie in (0, 1): $nackRateThreshold")
  require(nackRateThreshold > 0, s"nackRateThreshold must lie in (0, 1): $nackRateThreshold")

  private[this] val acceptRateThreshold: Double = 1.0 - nackRateThreshold
  private[this] val multiplier: Double = 1D / acceptRateThreshold
  private[this] val windowInNs: Long = window.inNanoseconds

  // Tracks the number of requests attempted during the previous 1000 ms. In
  // other words, tracks the client's rps. We arbitrarily give the Adder 10
  // slices.
  private[this] val rpsCounter: WindowedAdder = WindowedAdder(1000, 10, Stopwatch.systemMillis)

  // EMA representing the rate of responses that are not nacks. We update it
  // whenever we get a response from the cluster with 0 when the service responds
  // with a nack and 1 otherwise.
  // NB: Usage of the ema must be synchronized with the generation of the timestamp.
  //     and neither the Ema nor Monotime class is threadsafe.
  private[this] val ema: Ema = new Ema(windowInNs)
  // Start the ema at 1.0. No need for synchronization during construction.
  ema.update(monoTime.nanos(), 1)

  // visible for testing. Synchronized as Ema is not threadsafe
  private[filter] def emaValue: Double = synchronized { ema.last }

  private[this] val droppedRequestCounter: Counter = statsReceiver.counter("dropped_requests")
  private[this] val emaPercent: Gauge = statsReceiver.addGauge(Verbosity.Debug, "ema_value") {
    (emaValue * 100).toFloat
  }

  // Decrease the EMA if the response is a Nack, increase otherwise. Update the
  // acceptFraction & last update time.
  private[this] val afterSend: Try[Rep] => Unit = (rep: Try[Rep]) => {
    val value =
      if (sufficientRps) rep match {
        case Throw(f: Failure) if f.isFlagged(Failure.Rejected) => 0
        case _ => 1
      } else {
        // Bump up the ema value if the rps is too low to drop the request.
        // The ema value will start off high when the rps rises, protecting
        // against prematurely dropping requests (e.g., during warmup).
        1
      }

    // Avoid a race condition where another update occurs between the call to
    // nanos and the update.
    synchronized { ema.update(monoTime.nanos(), value) }
  }

  // Determines whether the client's rps is high enough to lower the ema
  // value and drop requests.
  private[this] def sufficientRps: Boolean = {
    rpsCounter.sum() >= rpsThreshold
  }

  // Drop the current request if:
  // 1. the accept fraction is under the threshold, and
  // 2. a random value is < the calculated drop probability.
  private[this] def shouldDropRequest(): Boolean = {
    val acceptFraction = emaValue
    acceptFraction < acceptRateThreshold &&
    random.nextDouble() < math.min(MaxDropProbability, 1.0 - multiplier * acceptFraction)
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    rpsCounter.incr()
    if (enableNackAc() && shouldDropRequest()) {
      droppedRequestCounter.incr()
      OverloadFailure
    } else {
      service(req).respond(afterSend)
    }
  }
}
