package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.service.RetryPolicy.RetryableWriteException
import com.twitter.finagle.service.{ReqRep, ResponseClass, _}
import com.twitter.util.{Duration, Future, Throw, Try}

private[finagle] object MethodBuilder {

  /**
   * Note that metrics will be scoped (e.g. "clnt/your_client_label")
   * to the `withLabel` setting (from [[param.Label]]). If that is
   * not set, `dest` is used.
   *
   * @param dest where requests are dispatched to.
   *             See the [[http://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def from[Req, Rep](
    dest: String,
    stackClient: StackClient[Req, Rep]
  ): MethodBuilder[Req, Rep] = {
    val needsTotalTimeoutModule =
      stackClient.stack.contains(TimeoutFilter.totalTimeoutRole)
    val service: Service[Req, Rep] = stackClient
      .withStack(modified(stackClient.stack))
      .withParams(stackClient.params)
      .newService(dest, param.Label.Default)
    new MethodBuilder[Req, Rep](
      service,
      stackClient.params,
      Config(needsTotalTimeoutModule))
  }

  private[this] def modified[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]]
  ): Stack[ServiceFactory[Req, Rep]] = {
    // this is managed directly by us, so that we can put it in the right location
    stack.remove(TimeoutFilter.totalTimeoutRole)
  }

  /** Exposed for testing purposes */
  private[client] val RetryPolicyMaxRetries = 2

  private case class Config[Req, Rep](
      // indicates the stack originally had a total timeout module.
      // if `totalTimeout` does not get overridden by the module, it must
      // still be added back.
      stackHadTotalTimeout: Boolean,

      // which req/rep pairs should be retried
      retryPolicy: RetryPolicy[(Req, Try[Rep])] = RetryPolicy.none,

      // this includes retries, connection setup, etc
      timeoutTotal: Duration = Duration.Undefined)

}

/**
 * '''Experimental:''' This API is under construction.
 *
 * @define retries
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
 * to retry via [[RequeueFilter]]. This includes [[WriteException WriteExceptions]]
 * and [[Failure.Restartable retryable nacks]]. As these should have already
 * been retried, we avoid retrying them again by ignoring them at this layer.
 *
 * Additional information regarding retries can be found in the
 * [[https://twitter.github.io/finagle/guide/Clients.html#retries user guide]].
 */
private[finagle] class MethodBuilder[Req, Rep] private (
    service: Service[Req, Rep],
    params: Stack.Params,
    config: MethodBuilder.Config[Req, Rep]) { self =>
  import MethodBuilder._

  //
  // Configuration
  //

  /**
   * Set a total timeout, including time spent on retries.
   *
   * If the request does not complete in this time, the response
   * will be satisfied with a [[GlobalRequestTimeoutException]].
   *
   * @param howLong how long, from the initial request issuance,
   *                is the request given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   */
  def withTimeoutTotal(howLong: Duration): MethodBuilder[Req, Rep] =
    withConfig(config.copy(timeoutTotal = howLong))

  /**
   * $retries
   *
   * Retry based on [[ResponseClassifier]].
   *
   * @param classifier when a [[ResponseClass.Failed Failed]] with `retryable`
   *                   is `true` is returned for a given `ReqRep`, the
   *                   request will be retried.
   *                   This is often [[ResponseClass.RetryableFailure]].
   *
   * @see [[withRetriesForResponse]]
   * @see [[withRetriesForReqRep]]
   */
  def withRetriesForClassifier(
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
    withRetriesForReqRep(shouldRetry)
  }

  /**
   * $retries
   *
   * Retry based on responses.
   *
   * @param shouldRetry when `true` for a given response,
   *                    the request will be retried.
   *
   * @see [[withRetriesForReqRep]]
   * @see [[withRetriesForClassifier]]
   */
  def withRetriesForResponse(
    shouldRetry: PartialFunction[Try[Rep], Boolean]
  ): MethodBuilder[Req, Rep] = {
    val shouldRetryWithReq = new PartialFunction[(Req, Try[Rep]), Boolean] {
      def isDefinedAt(reqRep: (Req, Try[Rep])): Boolean =
        shouldRetry.isDefinedAt(reqRep._2)
      def apply(reqRep: (Req, Try[Rep])): Boolean =
        shouldRetry(reqRep._2)
    }
    withRetriesForReqRep(shouldRetryWithReq)
  }

  /**
   * $retries
   *
   * Retry based on the request and response.
   *
   * @param shouldRetry when `true` for a given request and response,
   *                    the request will be retried.
   *
   * @see [[withRetriesForResponse]]
   * @see [[withRetriesForClassifier]]
   */
  def withRetriesForReqRep(
    shouldRetry: PartialFunction[(Req, Try[Rep]), Boolean]
  ): MethodBuilder[Req, Rep] = {
    val policy = RetryPolicy.tries(
      RetryPolicyMaxRetries + 1, // add 1 for the initial request
      shouldRetry)
    withRetriesForPolicy(policy)
  }

  /**
   * '''Expert API:''' For most users, the other `withRetriesFor`
   * APIs should suffice as they provide good defaults.
   *
   * $retries
   *
   * Retry based on a [[RetryPolicy]].
   *
   * @see [[withRetriesForResponse]]
   * @see [[withRetriesForReqRep]]
   * @see [[withRetriesForClassifier]]
   */
  def withRetriesForPolicy(
    retryPolicy: RetryPolicy[(Req, Try[Rep])]
  ): MethodBuilder[Req, Rep] = {
    val withoutRequeues = retryPolicy.filterEach[(Req, Try[Rep])] {
      // rely on finagle to handle these in the RequeueFilter
      // and avoid retrying them again.
      case (_, Throw(RetryableWriteException(_))) => false
      case _ => true
    }
    withConfig(config.copy(retryPolicy = withoutRequeues))
  }

  //
  // Build
  //

  /**
   * Create a [[Service]] from the current configuration.
   *
   * @param name used for scoping metrics
   */
  def newService(name: String): Service[Req, Rep] =
    filter(name).andThen(service)

  //
  // Internals
  //

  private[this] def withConfig(config: Config[Req, Rep]): MethodBuilder[Req, Rep] =
    new MethodBuilder(self.service, self.params, config)

  private[this] def filter(name: String): Filter[Req, Rep, Req, Rep] = {
    // Ordering of filters:
    // Requests start at the top and traverse down.
    // Responses flow back from the bottom up.
    //
    // - Failure recovery (TODO)
    // - Logical Stats (TODO)
    // - Total Timeout
    // - Retries
    // - Service (Finagle client's stack, including Per Request Timeout (TODO))

    val stats = params[param.Stats].statsReceiver.scope(name)

    // ko todo: consider making these Modules so that they appear in the registry

    val totalTimeoutFilter =
      if (!config.timeoutTotal.isFinite) {
        if (config.stackHadTotalTimeout)
          DynamicTimeout.totalFilter[Req, Rep](params) // use their defaults
        else
          Filter.identity[Req, Rep]
      } else {
        val dyn = new SimpleFilter[Req, Rep] {
          def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
            DynamicTimeout.letTotalTimeout(config.timeoutTotal) {
              service(req)
            }
          }
        }
        dyn.andThen(DynamicTimeout.totalFilter[Req, Rep](params))
      }

    val retryFilter =
      if (config.retryPolicy == RetryPolicy.none) Filter.identity[Req, Rep]
      else new RetryFilter[Req, Rep](
        config.retryPolicy,
        params[param.HighResTimer].timer,
        stats,
        params[Retries.Budget].retryBudget)

    totalTimeoutFilter.andThen(retryFilter)
  }

}
