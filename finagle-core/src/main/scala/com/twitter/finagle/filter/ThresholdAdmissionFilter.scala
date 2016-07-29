package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{Ema, Rng}
import com.twitter.logging.{Level, Logger}
import com.twitter.util._

private[finagle] object ThresholdAdmissionFilter {
  private val overloadFailure = Future.exception(Failure("failed fast because service is overloaded"))
  private val logger = Logger.get(getClass)
}

/**
 * This filter probabilistically drops requests if the success rate falls
 * below an arbitrary threshold, which is determined by the `multiplier`.
 * In the case that most or all of the cluster which the client is speaking
 * to is overloaded, this will help the cluster cool off.
 *
 * The implementation of this filter is heavily inspired by the O'Reilly's
 * "Site Reliability Engineering: How Google Runs Production Systems", by
 * Beyer, Jones, Petoff, and Murphy, 1e.
 *
 * @param window Size of moving window for exponential moving average, which is
 * used to keep track of the ratio of failed responses to successful responses
 * and compute the client's success rate. Note that the window is not a
 * [[Duration]]; in other words, we can interpret its unit as representing
 * event time, not system time. For example, if set to 1, then only the most
 * recent response will be used to calculate the EMA. If set to 100, then the
 * past 100 responses will be used to calculate the EMA. The window size
 * influences how the EMA behaves in roughly the following ways:
 *
 * - an EMA with a smaller window size will respond more quickly to
 * changes in cluster performance, but will keep a less accurate
 * estimate of the long-term average success rate;
 *
 * - an EMA with a larger window size will respond more slowly to changes in
 * cluster performance, but will keep a more accurate estimate of the long-
 * term average success rate.
 *
 * TODO: Include guidance for choosing a specific window depending on service
 *       characteristics.
 *
 * @param multiplier Constant which determines how aggressively the filter
 * drops requests. For example, if set to 2, then the lowest success rate
 * the filter will tolerate before probabilistically dropping requests is 50%;
 * if set to 3, then the lowest success rate tolerated is 33.3%. In general,
 * if set to x, the lowest success rate tolerated is 1/x.
 *
 * @param random Random number generator used in probability calculation.
 */
private[finagle] class ThresholdAdmissionFilter[Req, Rep](
    window: Duration,
    multiplier: Double,
    random: Rng,
    statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep] {
  import ThresholdAdmissionFilter._

  private val windowInMs = window.inMilliseconds
  require(windowInMs > 0, s"window must be positive: $windowInMs")
  require(multiplier > 1, s"multiplier must be greater than 1: $multiplier")

  // EMA representing the success rate. We update it whenever we get a response
  // from the cluster via [[updateSuccessLikelihood]]. We update it with 1 when
  // we get a successful response and with 0 when we get a failure response.
  val successLikelihoodEma = new Ema(windowInMs)

  // Boolean representing whether the first request has been sent.
  var sentRequest = false

  // EMA timestamp. Uses a time delta of 1. For example, when we send the first
  // request, we increment the stamp to 1; when we send the second request, we
  // increment the stamp to 2.
  private var successLikelihoodStamp: Long = 0

  val fastFailureCounter = statsReceiver.counter("fastFailures")
  val successLikelihoodHistogram = statsReceiver.stat("successLikelihood")

  private def updateSuccessLikelihood(v: Long) = {
    synchronized {
      successLikelihoodEma(successLikelihoodStamp) = v
      successLikelihoodStamp += 1
    }
    successLikelihoodHistogram.add(successLikelihoodEma.last.toFloat)
  }

  // Serve the given request, and update [[successLikelihoodEma]] depending on the
  // response status.
  // TODO: Use Response Classification.
  private val afterSend: Try[Rep] => Unit = {
    case Return(_) =>
      updateSuccessLikelihood(1)
    case Throw(_) =>
      updateSuccessLikelihood(0)
  }

  private def sendRequest(req: Req, service: Service[Req, Rep]): Future[Rep] = {
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
        fastFailureCounter.incr()
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