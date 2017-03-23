package com.twitter.finagle.client

import com.twitter.finagle.service.ResponseClassifier
import com.twitter.util.Duration

/**
 * '''Experimental:''' This API is under construction.
 *
 * = Timeouts =
 *
 * Defaults to using the StackClient's configuration.
 *
 * An example of setting a per-request timeout of 50 milliseconds and a total
 * timeout of 100 milliseconds:
 * {{{
 * import com.twitter.conversions.time._
 * import com.twitter.finagle.client.MethodBuilderScaladoc
 *
 * val builder: MethodBuilderScaladoc[_] = ???
 * builder
 *   .withTimeoutPerRequest(50.milliseconds)
 *   .withTimeoutTotal(100.milliseconds)
 * }}}
 *
 * = Retries =
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
 * See [[MethodBuilderScaladoc.withRetryForClassifier]] for details.
 *
 * A [[com.twitter.finagle.service.RetryBudget]] is used to prevent retries from overwhelming
 * the backend service. The budget is shared across clients created from
 * an initial [[MethodBuilder]]. As such, even if the retry rules
 * deem the request retryable, it may not be retried if there is insufficient
 * budget.
 *
 * Finagle will automatically retry failures that are known to be safe
 * to retry via [[com.twitter.finagle.service.RequeueFilter]]. This includes
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
 * Logical success rate metrics are scoped to
 * "clnt/your_client_label/method_name/logical" and get "success" and
 * "requests" counters along with a "request_latency_ms" stat.
 *
 * @note This interface primarily exists to share Scaladocs across
 *       protocol-specific `MethodBuilder` implementations.
 */
private[finagle] trait MethodBuilderScaladoc[T] {

  /**
   * Set a total timeout, including time spent on retries.
   *
   * If the request does not complete in this time, the response
   * will be satisfied with a [[com.twitter.finagle.GlobalRequestTimeoutException]].
   *
   * Defaults to using the client's configuration for
   * [[com.twitter.finagle.service.TimeoutFilter.TotalTimeout(timeout)]].
   *
   * @example
   * For example, a total timeout of 200 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.http.Http
   *
   * val client: Http.Client = ???
   * val builder = client.methodBuilder("inet!example.com:80")
   * builder.withTimeoutTotal(200.milliseconds))
   * }}}
   *
   * @param howLong how long, from the initial request issuance,
   *                is the request given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   *
   * @see [[withTimeoutPerRequest(Duration)]]
   *
   * @return a new instance with all other settings copied
   */
  def withTimeoutTotal(howLong: Duration): T

  /**
   * How long a '''single''' request is given to complete.
   *
   * If there are [[withRetryForClassifier retries]], each attempt is given up
   * to this amount of time.
   *
   * If a request does not complete within this time, the response
   * will be satisfied with a [[com.twitter.finagle.IndividualRequestTimeoutException]].
   *
   * Defaults to using the client's configuration for
   * [[com.twitter.finagle.service.TimeoutFilter.Param(timeout)]],
   * which is typically set via
   * [[com.twitter.finagle.param.CommonParams.withRequestTimeout]].
   *
   * @example
   * For example, a per-request timeout of 50 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.http.Http
   *
   * val client: Http.Client = ???
   * val builder = client.methodBuilder("inet!example.com:80")
   * builder.withTimeoutPerRequest(50.milliseconds))
   * }}}
   *
   * @param howLong how long, from the initial request issuance,
   *                an individual attempt given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   *
   * @see [[withTimeoutTotal(Duration)]]
   *
   * @return a new instance with all other settings copied
   */
  def withTimeoutPerRequest(howLong: Duration): T

  /**
   * Retry based on [[ResponseClassifier]].
   *
   * The default behavior is to use the client's classifier which is typically
   * configured through `theClient.withResponseClassifier` or
   * `ClientBuilder.withResponseClassifier`.
   *
   * @example
   * For example, retrying on a 418 status code:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.http.Http
   * import com.twitter.finagle.service.{ReqRep, ResponseClass}
   * import com.twitter.util.Return
   *
   * val client: Http.Client = ???
   * val builder = client.methodBuilder("inet!example.com:80")
   * builder.withRetryForClassifier {
   *   case ReqRep(_, Return(rep)) if rep.statusCode == 418 => ResponseClass.RetryableFailure
   * }
   * }}}
   *
   * The classifier is also used to determine the logical success metrics of
   * the client.
   *
   * @param classifier when a [[com.twitter.finagle.service.ResponseClass.Failed Failed]]
   *                   with `retryable` is `true` is returned for a given `ReqRep`, the
   *                   request will be retried. This is often a
   *                   `ResponseClass.RetryableFailure`.
   *
   * @see [[withRetryDisabled]]
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#response-classification
   *     response classification user guide]]
   */
  def withRetryForClassifier(classifier: ResponseClassifier): T

  /**
   * Disables "application" level retries.
   *
   * This '''does not''' disable retries of failures that are known
   * to be safe to retry via `com.twitter.finagle.service.RequeueFilter`.
   *
   * This causes the logical success metrics to be based on the
   * [[ResponseClassifier.Default default response classifier]] rules
   * of a `Return` response is a success, while everything else is not.
   *
   * @example
   * {{{
   * import com.twitter.finagle.http.Http
   *
   * val client: Http.Client = ???
   * val builder = client.methodBuilder("inet!example.com:80")
   * builder.withRetryDisabled
   * }}}
   *
   * @see [[withRetryForClassifier]]
   */
  def withRetryDisabled: T

}
