package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{Ema, Rng}
import com.twitter.logging.{Level, Logger}
import com.twitter.util._

private[finagle] object NackAdmissionFilter {
  private val overloadFailure = Future.exception(Failure("failed fast because service is overloaded"))
  private val logger = Logger.get(getClass)
  private val role = new Stack.Role("NackAdmissionFilter")

  /**
   * By default, the EMA window is 2 minutes: any response that the filter
   * receives over a 2 minute rolling window affects the EMA's value. In other
   * words, the EMA "forgets" history older than 2 minutes.
   *
   * E.g., if the server fails every request received for one minute, then
   * two minutes pass without the server receiving any requests, and then
   * the server receives a small number of successes, the EMA will end up
   * being very close to 1.
   */
  private val DefaultWindow = 2.minutes

  /**
   * By default, the client will send all requests when the success rate EMA is
   * between 50% and 100%. If the EMA drops below 50%, the filter will drop any
   * given request with probability proportional to the EMA.
   *
   * E.g., if the EMA is 20%, the filter will drop any given request with
   * 100 - (2 * 20) = 60% probability.
   */
  private val DefaultSuccessRateThreshold = 0.5

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.NackAdmissionFilter]] module.
   */
  case class Param(window: Duration, successRateThreshold: Double) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param: Stack.Param[NackAdmissionFilter.Param] =
      Stack.Param(Param(DefaultWindow, DefaultSuccessRateThreshold))
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[
      NackAdmissionFilter.Param,
      param.Stats,
      ServiceFactory[Req, Rep]] {
      val description = "Probabilistically drops requests to the underlying service."
      val role = NackAdmissionFilter.role

      def make(
        _param: Param,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Stats(stats) = _stats

        // Create the filter with the given window and success rate threshold.
        val filter = new NackAdmissionFilter[Req, Rep](
          _param.window, _param.successRateThreshold, Rng.threadLocal,
          stats.scope("nack_admission_control"))

        // Insert the filter into the client stack. We use `FactoryToService`
        // and `ServiceFactory.const` to ensure that the filter operates across
        // all endpoints rather than just per-endpoint.
        ServiceFactory.const(filter.andThen(new FactoryToService(next)))
      }
    }
}

/**
 * This filter probabilistically drops requests if the success rate falls
 * below the `successRateThreshold`. In the case that most or all of the
 * cluster which the client is speaking to is overloaded, this will help the
 * cluster cool off.
 *
 * The implementation of this filter is heavily inspired by Chapter 21, section
 * "Client-Side Throttling" of O'Reilly's "Site Reliability Engineering: How
 * Google Runs Production Systems", by Beyer, Jones, Petoff, and Murphy, 1e.
 *
 * @param window Size of moving window for exponential moving average, which is
 * used to keep track of the ratio of failed responses to successful responses
 * and compute the client's success rate. E.g., if set to 1 second, then only
 * requests occurring over the previous second will be used to calculate the
 * EMA. The window size influences how the EMA behaves in roughly the following
 * ways:
 *
 * - an EMA with a smaller window size will respond more quickly to
 * changes in cluster performance, but will keep a less accurate
 * estimate of the long-term average success rate;
 *
 * - an EMA with a larger window size will respond more slowly to changes in
 * cluster performance, but will keep a more accurate estimate of the long-
 * term average success rate.
 *
 * @param successRateThreshold Constant which determines how aggressively the
 * filter drops requests. For example, if set to 1/2, then the lowest success
 * rate the filter will tolerate before probabilistically dropping requests is
 * 50%; if set to 1/3, then the lowest success rate tolerated is 33.3%. In
 * general, if set to x, the lowest success rate tolerated is x.
 *
 * @param random Random number generator used in probability calculation.
 */
private[finagle] class NackAdmissionFilter[Req, Rep](
    window: Duration,
    successRateThreshold: Double,
    random: Rng,
    statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep] {
  import NackAdmissionFilter._

  val windowInNs = window.inNanoseconds
  require(windowInNs > 0, s"window size must be positive: $windowInNs")
  require(successRateThreshold < 1, s"successRateThreshold must lie in (0, 1): $successRateThreshold")
  require(successRateThreshold > 0, s"successRateThreshold must lie in (0, 1): $successRateThreshold")

  private[this] val multiplier = 1D/successRateThreshold

  // EMA representing the success rate. We update it whenever we get a response
  // from the cluster via [[updateSuccessLikelihood]]. We update it with 1 when
  // we get a successful response and with 0 when we get a failure response.
  // Therefore its value is a Double in the range [0, 1].
  private[this] val successLikelihoodEma = new Ema(windowInNs)

  private[finagle] def emaValue: Double = {
    successLikelihoodEma.last
  }

  private[this] val monoTime = new Ema.Monotime

  // Boolean representing whether the first request has been sent.
  private[this] var sentRequest = false

  private[finagle] def hasSentRequest: Boolean = synchronized {
    sentRequest
  }

  private[this] val droppedRequestCounter = statsReceiver.counter("dropped_requests")
  private[this] val successProbabilityHistogram = statsReceiver.stat("success_probability")

  private[this] def updateSuccessLikelihood(v: Long) = {
    successLikelihoodEma.update(monoTime.nanos(), v)
    successProbabilityHistogram.add(successLikelihoodEma.last.toFloat)
  }

  // Serve the given request, and update [[successLikelihoodEma]] depending on the
  // response status.
  // TODO: Use Response Classification.
  private[this] val afterSend: Try[Rep] => Unit = {
    case Return(_) =>
      updateSuccessLikelihood(1)
    case Throw(_) =>
      updateSuccessLikelihood(0)
  }

  private[this] def sendRequest(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(req).respond(afterSend)
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val sentFirstRequest = synchronized { sentRequest }
    val currentSuccessLikelihoodEma = successLikelihoodEma.last
    val successProduct = multiplier * currentSuccessLikelihoodEma

    // Probabilistically drop requests if the success rate is below the failure
    // threshold.
    val res = if (sentFirstRequest && successProduct < 1) {
      val randomDouble = random.nextDouble()
      val failureProbability = math.max(0, 1 - successProduct)
      if (randomDouble >= failureProbability) {
        sendRequest(req, service)
      } else {
        droppedRequestCounter.incr()
        if (logger.isLoggable(Level.DEBUG)) {
          logger.debug(s"""Dropping request\tsuccessLikelihood=$currentSuccessLikelihoodEma\tsuccessProduct=$successProduct""")
        }
        overloadFailure
      }
    } else {
      sendRequest(req, service)
    }

    // Update [[sentRequest]] only after sending the first request. It is
    // initially `false` to avoid dropping the first request, and it is always
    // true after we send the first request.
    if (!sentFirstRequest) {
      res.ensure {
        synchronized {
          sentRequest = true
        }
      }
    }
    res
  }
}
