package com.twitter.finagle.client

import com.twitter.finagle.{Filter, param}
import com.twitter.finagle.service.{RequeueFilter, _}
import com.twitter.finagle.stats.{BlacklistStatsReceiver, ExceptionStatsHandler, StatsReceiver}
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
 * The classifier is also used to determine the logical success metrics of
 * the client. Logical here means after any retries are run. For example
 * should a request result in retryable failure on the first attempt, but
 * succeed upon retry, this is exposed through metrics as a success.
 *
 * @note Retries can be disabled using [[disabled]]:
 * {{{
 * import com.twitter.finagle.client.MethodBuilder
 *
 * val methodBuilder: MethodBuilder[Int, Int] = ???
 * methodBuilder.withRetry.disabled
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
   * The classifier is also used to determine the logical success metrics of
   * the client.
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
    mb.withConfig(mb.config.copy(retry = Config(classifier)))

  /**
   * Disables retries.
   *
   * This '''does not''' disable retries of failures that are known
   * to be safe to retry via [[RequeueFilter]].
   *
   * This causes the logical success metrics to be based on the
   * [[ResponseClassifier.Default default response classifier]] rules
   * of a `Return` response is a success, while everything else is not.
   */
  def disabled: MethodBuilder[Req, Rep] =
    forClassifier(Disabled)

  private[client] def filter(
    scopedStats: StatsReceiver
  ): Filter.TypeAgnostic = {
    val classifier = mb.config.retry.responseClassifier
    if (classifier eq Disabled)
      Filter.TypeAgnostic.Identity
    else {
      new Filter.TypeAgnostic {
        def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = {
          val retryPolicy = policyForReqRep(shouldRetry[Req1, Rep1](classifier))
          val withoutRequeues = filteredPolicy(retryPolicy)

          new RetryFilter[Req1, Rep1](
            withoutRequeues,
            mb.params[param.HighResTimer].timer,
            scopedStats,
            mb.params[Retries.Budget].retryBudget)
        }
      }
    }
  }

  private[client] def logicalStatsFilter(stats: StatsReceiver): Filter.TypeAgnostic =
    StatsFilter.typeAgnostic(
      new BlacklistStatsReceiver(stats.scope(LogicalScope), LogicalStatsBlacklistFn),
      mb.config.retry.responseClassifier,
      ExceptionStatsHandler.Null,
      mb.params[StatsFilter.Param].unit)

  private[client] def registryEntries: Iterable[(Seq[String], String)] = {
    Seq(
      (Seq("retry"), mb.config.retry.toString)
    )
  }

}

private[client] object MethodBuilderRetry {
  val MaxRetries = 2

  private val Disabled: ResponseClassifier =
    ResponseClassifier.named("Disabled")(PartialFunction.empty)

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

  /** The stats scope used for logical success rate. */
  private val LogicalScope = "logical"

  // the `StatsReceiver` used is already scoped to `$clientName/$methodName/logical`.
  // this omits the pending gauge as well as failures/sourcedfailures details.
  private val LogicalStatsBlacklistFn: Seq[String] => Boolean = { segments =>
    val head = segments.head
    head == "pending" || head == "failures" || head == "sourcedfailures"
  }

  /**
   * @see [[MethodBuilderRetry.forClassifier]] for details on how the
   *     classifier is used.
   */
  case class Config(responseClassifier: ResponseClassifier)

}
