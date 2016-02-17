package com.twitter.finagle.param

import com.twitter.finagle.service.{Retries, RetryBudget}
import com.twitter.finagle.{service, Stack}
import com.twitter.util.Duration

/**
 * A collection of methods for basic configuration of Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html]]
 */
trait ClientParams[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Configures the [[RetryBudget retry budget]] of this client (default:
   * allows for about 20% of the total requests to be retried on top of
   * 10 retries per second).
   *
   * This `budget` is shared across requests and governs the number of
   * retries that can be made by this client.
   *
   * @note The retry budget helps prevent clients from overwhelming the
   *       downstream service.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#retries]]
   */
  def withRetryBudget(budget: RetryBudget): A =
    self.configured(self.params[Retries.Budget].copy(retryBudget = budget))

  /**
   * Configures the requeue backoff policy of this client (default: no delay).
   *
   * The `backoff` policy is represented by a stream of delays (i.e.,
   * `Stream[Duration]`) used to delay each retry.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#retries]]
   */
  def withRetryBackoff(backoff: Stream[Duration]): A =
    self.configured(self.params[Retries.Budget].copy(requeueBackoffs = backoff))

  /**
   * Configures the [[service.ResponseClassifier Response Classifier]] of this client,
   * which is used to determine the result of a request/response.
   *
   * This allows developers to give Finagle the additional application-specific
   * knowledge necessary in order to properly classify them. Without this,
   * Finagle cannot make judgements about application level failures as it only
   * has a narrow understanding of failures (for example: transport level, timeouts,
   * and NACKs).
   *
   * As an example take an HTTP client that receives a response with a 500 status
   * code back from a server. To Finagle this is a successful request/response
   * based solely on the transport level. The application developer may want to
   * treat all 500 status codes as failures and can do so via a
   * [[service.ResponseClassifier Response Classifier]].
   *
   * It is a [[PartialFunction]] and as such multiple classifiers can be composed
   * together via [[PartialFunction.orElse]].
   *
   * @see `com.twitter.finagle.http.service.HttpResponseClassifier` for some
   *      HTTP classification tools.
   *
   * @note If unspecified, the default classifier is [[service.ResponseClassifier.Default]]
   *       which is a total function fully covering the input domain.
   */
  def withResponseClassifier(responseClassifier: service.ResponseClassifier): A =
    self.configured(ResponseClassifier(responseClassifier))
}
