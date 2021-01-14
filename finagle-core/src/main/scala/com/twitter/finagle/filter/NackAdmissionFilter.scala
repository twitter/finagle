package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.useNackAdmissionFilter
import com.twitter.finagle.stats.{Counter, Gauge, StatsReceiver, Verbosity}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.{LossyEma, Rng}
import com.twitter.util._

object NackAdmissionFilter {
  private val OverloadFailure = Future.exception(
    Failure(
      "Request not issued to the backend due to observed overload.",
      FailureFlags.Rejected | FailureFlags.NonRetryable
    )
  )
  val role: Stack.Role = Stack.Role("NackAdmissionFilter")

  private val enabled: Boolean = useNackAdmissionFilter()

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
  val DefaultWindow: Duration = 2.minutes

  /**
   * By default, the client will send all requests when the accept rate EMA is
   * between 50% and 100%. If the EMA drops below 50%, the filter will drop any
   * given request with probability proportional to the EMA.
   *
   * E.g., if the EMA is 20%, the filter will drop any given request with
   * 100 - (2 * 20) = 60% probability. If the EMA is 10%, the filter will drop
   * any given request with 100 - (2 * 10) = 80% probability.
   */
  val DefaultNackRateThreshold: Double = 0.5

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

  object Param {

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
      ): ServiceFactory[Req, Rep] = _param match {
        case Param.Configured(window, threshold) if enabled =>
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

        case _ =>
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
 * NOTE: Here is a brief summary of the configurable params.
 *
 * A configuration with a `nackRateThreshold` of N% and a `window` of duration
 * W roughly translates as, "start dropping some requests to the cluster when
 * the nack rate averages at least N% over a window of duration W."
 *
 * Here are some examples of situations with param values chosen to make the
 * filter useful:
 *
 * - Owners of Service A examine their service's nack rate over several days
 *   and find that it is almost always under 10% and rarely above 1% (e.g.,
 *   during traffic spikes) or 5% (e.g., during a data center outage). They
 *   do not want to preemptively drop requests unless the cluster sees an
 *   extreme overload situation so they choose a nack rate threshold of 20%.
 *   And in such a situation they want the filter to act relatively quickly,
 *   so they choose a window of 30 seconds.
 *
 * - Owners of Service B observe that excess load typically causes peak nack
 *   rates of around 25% for up to 60 seconds. They want to be aggressive
 *   about avoiding cluster overload and don’t mind dropping some innocent
 *   requests during mild load so they choose a window of 10 seconds and a
 *   threshold of 0.15 (= 15%).
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
class NackAdmissionFilter[Req, Rep] private[filter] (
  window: Duration,
  nackRateThreshold: Double,
  random: Rng,
  statsReceiver: StatsReceiver,
  now: () => Long)
    extends SimpleFilter[Req, Rep] {
  import NackAdmissionFilter._

  def this(window: Duration, nackRateThreshold: Double, random: Rng, statsReceiver: StatsReceiver) =
    this(window, nackRateThreshold, random, statsReceiver, Stopwatch.systemNanos)

  require(window > Duration.Zero, s"window size must be positive: $window")
  require(nackRateThreshold < 1, s"nackRateThreshold must lie in (0, 1): $nackRateThreshold")
  require(nackRateThreshold > 0, s"nackRateThreshold must lie in (0, 1): $nackRateThreshold")

  private[this] val acceptRateThreshold: Double = 1.0 - nackRateThreshold
  private[this] val multiplier: Double = 1d / acceptRateThreshold

  // Tracks the number of requests attempted during the previous 1000 ms. In
  // other words, tracks the client's rps. We arbitrarily give the Adder 10
  // slices.
  private[this] val rpsCounter: WindowedAdder = WindowedAdder(1000, 10, Stopwatch.systemMillis)

  // moving average representing the rate of responses that are not nacks. update it
  // whenever we get a response from the cluster with 0 when the service responds
  // with a nack and 1 otherwise. Start the moving average out with no nacks (1.0)
  private[this] val ema = new LossyEma(window.inNanoseconds, now, 1.0)

  // visible for testing.
  private[filter] def emaValue: Double = ema.last

  private[this] val droppedRequestCounter: Counter = statsReceiver.counter("dropped_requests")
  private[this] val emaPercent: Gauge = statsReceiver.addGauge(Verbosity.Debug, "ema_value") {
    (emaValue * 100.0).toFloat
  }

  // Decrease the EMA if the response is a Nack, increase otherwise. Update the
  // acceptFraction & last update time.
  private[this] val afterSend: Try[Rep] => Unit = (rep: Try[Rep]) => {
    val value =
      if (sufficientRps) rep match {
        case Throw(f: FailureFlags[_]) if f.isFlagged(FailureFlags.Rejected) => 0
        case _ => 1
      }
      else {
        // Bump up the ema value if the rps is too low to drop the request.
        // The ema value will start off high when the rps rises, protecting
        // against prematurely dropping requests (e.g., during warmup).
        1
      }

    ema.update(value)
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
    if (enabled && shouldDropRequest()) {
      val tracing = Trace()
      if (tracing.isActivelyTracing)
        tracing.recordBinary(
          "clnt/NackAdmissionFilter_rejected",
          s"probabilistically dropped because nackRate ${1d - emaValue} over window $window exceeds nackRateThreshold $acceptRateThreshold"
        )
      droppedRequestCounter.incr()
      OverloadFailure
    } else {
      service(req).respond(afterSend)
    }
  }
}
