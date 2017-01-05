package com.twitter.finagle.client

import com.twitter.finagle.{Filter, Stack, param}
import com.twitter.finagle.service.RequeueFilter
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
 * Defaults to using the client's [[ResponseClassifier]] to retry failures
 * [[com.twitter.finagle.service.ResponseClass.RetryableFailure marked as retryable]].
 * See [[forClassifier]] for details.
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
 * @note Retries can be disabled using [[RetryPolicy.none]]:
 * {{{
 * import com.twitter.finagle.client.MethodBuilder
 * import com.twitter.finagle.service.RetryPolicy
 *
 * val methodBuilder: MethodBuilder[Int, Int] = ???
 * methodBuilder.withRetry.forPolicy(RetryPolicy.none)
 * }}}
 *
 * @see [[MethodBuilder.withRetry]]
 */
private[finagle] class MethodBuilderRetry[Req, Rep] private[client] (
    mb: MethodBuilder[Req, Rep]) {

  import MethodBuilderRetry._

  /**
   * Retry based on [[ResponseClassifier]].
   *
   * The default behavior is to use the client's classifier which is typically
   * configured through `theClient.withResponseClassifier` or
   * `ClientBuilder.withResponseClassifier`.
   *
   * For example, retrying on `Exception` responses:
   * {{{
   * import com.twitter.finagle.client.MethodBuilder
   * import com.twitter.finagle.service.{ReqRep, ResponseClass}
   * import com.twitter.util.Throw
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withRetry.forClassifier {
   *   case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
   * }
   * }}}
   *
   * @param classifier when a [[ResponseClass.Failed Failed]] with `retryable`
   *                   is `true` is returned for a given `ReqRep`, the
   *                   request will be retried.
   *                   This is often [[ResponseClass.RetryableFailure]].
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#response-classification
   *     response classification user guide]]
   */
  def forClassifier(
    classifier: ResponseClassifier
  ): MethodBuilder[Req, Rep] =
    forPolicy(policyForReqRep(shouldRetry(classifier)))

  /**
   * '''Expert API:''' For most users, the default policy which uses
   * the client's [[ResponseClassifier]] is appropriate.
   *
   * Retry based on a [[RetryPolicy]].
   *
   * @note Retries can be disabled using [[RetryPolicy.none]]:
   * {{{
   * import com.twitter.finagle.client.MethodBuilder
   * import com.twitter.finagle.service.RetryPolicy
   *
   * val methodBuilder: MethodBuilder[Int, Int] = ???
   * methodBuilder.withRetry.forPolicy(RetryPolicy.none)
   * }}}
   *
   * @see [[forClassifier]]
   */
  def forPolicy(
    retryPolicy: RetryPolicy[(Req, Try[Rep])]
  ): MethodBuilder[Req, Rep] = {
    val withoutRequeues = filteredPolicy(retryPolicy)
    val retries = mb.config.retry.copy(retryPolicy = withoutRequeues)
    mb.withConfig(mb.config.copy(retry = retries))
  }

  private[client] def filter(
    scopedStats: StatsReceiver
  ): Filter[Req, Rep, Req, Rep] = {
    val config = mb.config.retry
    if (!isDefined(config.retryPolicy))
      Filter.identity[Req, Rep]
    else
      new RetryFilter[Req, Rep](
        config.retryPolicy,
        mb.params[param.HighResTimer].timer,
        scopedStats,
        mb.params[Retries.Budget].retryBudget)
  }

}

private[client] object MethodBuilderRetry {
  val MaxRetries = 2

  private def shouldRetry[Req, Rep](
    classifier: ResponseClassifier
  ): PartialFunction[(Req, Try[Rep]), Boolean] =
    new PartialFunction[(Req, Try[Rep]), Boolean] {
      def isDefinedAt(reqRep: (Req, Try[Rep])): Boolean =
        classifier.isDefinedAt(ReqRep(reqRep._1, reqRep._2))
      def apply(reqRep: (Req, Try[Rep])): Boolean =
        classifier(ReqRep(reqRep._1, reqRep._2)) match {
          case ResponseClass.Failed(retryable) => retryable
          case _ => false
        }
    }

  private def policyForReqRep[Req, Rep](
    shouldRetry: PartialFunction[(Req, Try[Rep]), Boolean]
  ): RetryPolicy[(Req, Try[Rep])] =
    RetryPolicy.tries(
      MethodBuilderRetry.MaxRetries + 1, // add 1 for the initial request
      shouldRetry)

  private def isDefined(retryPolicy: RetryPolicy[_]): Boolean =
    retryPolicy != RetryPolicy.none

  private def filteredPolicy[Req, Rep](
    retryPolicy: RetryPolicy[(Req, Try[Rep])]
  ): RetryPolicy[(Req, Try[Rep])] =
    if (!isDefined(retryPolicy))
      retryPolicy
    else
      retryPolicy.filterEach[(Req, Try[Rep])] {
        // rely on finagle to handle these in the RequeueFilter
        // and avoid retrying them again.
        case (_, Throw(RequeueFilter.Requeueable(_))) => false
        case _ => true
      }

  /**
   * Creates a [[Config]] using the stack's [[ResponseClassifier]]
   * as the basis for which responses should be retried.
   *
   * @see [[MethodBuilderRetry.forClassifier]] for details on how the
   *     classifier is used.
   */
  def newConfig[Req, Rep](params: Stack.Params): Config[Req, Rep] = {
    val classifier = params[param.ResponseClassifier].responseClassifier
    val should = shouldRetry(classifier)
    val policy = filteredPolicy(policyForReqRep(should))
    Config(policy)
  }

  /**
   * See [[newConfig]] for creating new instances.
   *
   * @param retryPolicy which request/response pairs should be retried.
   */
  case class Config[Req, Rep] private (
      retryPolicy: RetryPolicy[(Req, Try[Rep])])

}
