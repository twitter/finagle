package com.twitter.finagle.client

import com.twitter.finagle.{Filter, param}
import com.twitter.finagle.service.RetryPolicy.RetryableWriteException
import com.twitter.finagle.service._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Throw, Try}

/**
 * '''Experimental:''' This API is under construction.
 *
 * Retries are intended to help clients improve success rate by trying
 * failed requests additional times. Care must be taken by developers
 * to only retry when they are known to be safe to issue the request multiple
 * times. This is because the client cannot always be sure what the
 * backend service has done. An example of a request that is safe to
 * retry would be a read-only request.
 *
 * A [[RetryBudget]] is used to prevent retries from overwhelming
 * the backend service. The budget is shared across clients created from
 * an initial [[MethodBuilder]]. As such, even if the retry rules
 * deem the request retryable, it may not be retried if there is insufficient
 * budget.
 *
 * Finagle will automatically retry failures that are known to be safe
 * to retry via [[RequeueFilter]]. This includes
 * [[com.twitter.finagle.WriteException WriteExceptions]] and
 * [[com.twitter.finagle.Failure.Restartable retryable nacks]]. As these should have
 * already been retried, we avoid retrying them again by ignoring them at this layer.
 *
 * Additional information regarding retries can be found in the
 * [[https://twitter.github.io/finagle/guide/Clients.html#retries user guide]].
 *
 * @see [[MethodBuilder.withRetry]]
 */
private[finagle] class MethodBuilderRetry[Req, Rep] private[client] (
    mb: MethodBuilder[Req, Rep]) {

  /**
   * Retry based on [[ResponseClassifier]].
   *
   * @param classifier when a [[ResponseClass.Failed Failed]] with `retryable`
   *                   is `true` is returned for a given `ReqRep`, the
   *                   request will be retried.
   *                   This is often [[ResponseClass.RetryableFailure]].
   *
   * @see [[forResponse]]
   * @see [[forRequestResponse]]
   */
  def forClassifier(
    classifier: ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    val shouldRetry = new PartialFunction[(Req, Try[Rep]), Boolean] {
      def isDefinedAt(reqRep: (Req, Try[Rep])): Boolean =
        classifier.isDefinedAt(ReqRep(reqRep._1, reqRep._2))
      def apply(reqRep: (Req, Try[Rep])): Boolean =
        classifier(ReqRep(reqRep._1, reqRep._2)) match {
          case ResponseClass.Failed(retryable) => retryable
          case _ => false
        }
    }
    forRequestResponse(shouldRetry)
  }

  /**
   * Retry based on responses.
   *
   * @param shouldRetry when `true` for a given response,
   *                    the request will be retried.
   *
   * @see [[forRequestResponse]]
   * @see [[forClassifier]]
   */
  def forResponse(
    shouldRetry: PartialFunction[Try[Rep], Boolean]
  ): MethodBuilder[Req, Rep] = {
    val shouldRetryWithReq = new PartialFunction[(Req, Try[Rep]), Boolean] {
      def isDefinedAt(reqRep: (Req, Try[Rep])): Boolean =
        shouldRetry.isDefinedAt(reqRep._2)
      def apply(reqRep: (Req, Try[Rep])): Boolean =
        shouldRetry(reqRep._2)
    }
    forRequestResponse(shouldRetryWithReq)
  }

  /**
   * Retry based on the request and response.
   *
   * @param shouldRetry when `true` for a given request and response,
   *                    the request will be retried.
   *
   * @see [[forResponse]]
   * @see [[forClassifier]]
   */
  def forRequestResponse(
    shouldRetry: PartialFunction[(Req, Try[Rep]), Boolean]
  ): MethodBuilder[Req, Rep] = {
    val policy = RetryPolicy.tries(
      MethodBuilderRetry.MaxRetries + 1, // add 1 for the initial request
      shouldRetry)
    forPolicy(policy)
  }

  /**
   * '''Expert API:''' For most users, the other retry configuration
   * APIs should suffice as they provide good defaults.
   *
   * Retry based on a [[RetryPolicy]].
   *
   * @see [[forResponse]]
   * @see [[forRequestResponse]]
   * @see [[forClassifier]]
   */
  def forPolicy(
    retryPolicy: RetryPolicy[(Req, Try[Rep])]
  ): MethodBuilder[Req, Rep] = {
    val withoutRequeues = retryPolicy.filterEach[(Req, Try[Rep])] {
      // rely on finagle to handle these in the RequeueFilter
      // and avoid retrying them again.
      case (_, Throw(RetryableWriteException(_))) => false
      case _ => true
    }
    val retries = mb.config.retry.copy(retryPolicy = withoutRequeues)
    mb.withConfig(mb.config.copy(retry = retries))
  }

  private[client] def filter(
    scopedStats: StatsReceiver
  ): Filter[Req, Rep, Req, Rep] = {
    val config = mb.config.retry
    if (config.retryPolicy == RetryPolicy.none)
      Filter.identity[Req, Rep]
    else new RetryFilter[Req, Rep](
      config.retryPolicy,
      mb.params[param.HighResTimer].timer,
      scopedStats,
      mb.params[Retries.Budget].retryBudget)
  }

}

private[client] object MethodBuilderRetry {
  val MaxRetries = 2

  /**
   * @param retryPolicy which req/rep pairs should be retried
   */
  case class Config[Req, Rep](
      retryPolicy: RetryPolicy[(Req, Try[Rep])] = RetryPolicy.none)

}
