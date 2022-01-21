package com.twitter.finagle.http

import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.param.{Tracer => TracerParam}
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.{Filter, Name, Resolver, Service, client}
import com.twitter.util.Duration
import com.twitter.util.tunable.Tunable
import com.twitter.{finagle => ctf}

object MethodBuilder {

  /**
   * Create a [[MethodBuilder]] for a given destination.
   *
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[com.twitter.finagle.param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newService(String)]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   *
   * @see [[com.twitter.finagle.Http.Client.methodBuilder(String)]]
   */
  def from(dest: String, stackClient: StackClient[Request, Response]): MethodBuilder =
    from(Resolver.eval(dest), stackClient)

  /**
   * Create a [[MethodBuilder]] for a given destination.
   *
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[com.twitter.finagle.param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newService(String)]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   *
   * @see [[com.twitter.finagle.Http.Client.methodBuilder(Name)]]
   */
  def from(dest: Name, stackClient: StackClient[Request, Response]): MethodBuilder = {
    val initializer = HttpClientTraceInitializer.typeAgnostic(
      stackClient.params[TracerParam].tracer
    )
    val mb = client.MethodBuilder
      .from(dest, stackClient)
      .withTraceInitializer(initializer)
    new MethodBuilder(mb)
  }

  /**
   * '''NOTE:''' Prefer using [[ctf.Http.Client.methodBuilder]] over using
   * this approach to construction. The functionality is available through
   * [[ctf.Http.Client]] and [[MethodBuilder]] while addressing the various issues
   * of [[ClientBuilder]].
   *
   * Creates a [[MethodBuilder]] from the given [[ClientBuilder]].
   *
   * Note that metrics will be scoped (e.g. "clnt/clientbuilders_name/method_name").
   *
   * The value for "clientbuilders_name" is taken from the [[ClientBuilder.name]]
   * configuration, using "client" if unspecified.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newService]].
   *
   *  - The [[ClientBuilder.timeout]] configuration will be used as the default
   *  value for [[MethodBuilder.withTimeoutTotal]].
   *
   *  - The [[ClientBuilder.requestTimeout]] configuration will be used as the
   *  default value for [[MethodBuilder.withTimeoutPerRequest]].
   *
   *  - The [[ClientBuilder]] must have been constructed using
   *  [[ClientBuilder.stack]] passing an instance of a [[ctf.Http.Client]].
   *
   *  - The [[ClientBuilder]] metrics scoped to "tries" are not included
   *  as they are superseded by metrics scoped to "logical".
   *
   *  - The [[ClientBuilder]] retry policy will not be applied and must
   *  be migrated to using [[MethodBuilder.withRetryForClassifier]].
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#migrating-from-clientbuilder user guide]]
   */
  def from(
    clientBuilder: ClientBuilder[Request, Response, ClientConfig.Yes, _, _]
  ): MethodBuilder = {
    if (!clientBuilder.params.contains[ClientConfig.DestName])
      throw new IllegalArgumentException("ClientBuilder must be configured with a dest")
    val dest = clientBuilder.params[ClientConfig.DestName].name
    val client = clientBuilder.client.asInstanceOf[ctf.Http.Client]
    from(dest, client)
  }

}

/**
 * `MethodBuilder` is a collection of APIs for client configuration at
 * a higher level than the Finagle 6 APIs while improving upon the deprecated
 * [[ClientBuilder]]. `MethodBuilder` provides:
 *
 *  - Logical success rate metrics.
 *  - Retries based on application-level requests and responses (e.g. an HTTP 503 response code).
 *  - Configuration of per-attempt and total timeouts.
 *
 * All of these can be customized per method (or endpoint) while sharing a single
 * underlying Finagle client. Concretely, a single service might offer both
 * `GET statuses/show/:id` as well as `POST statuses/update`, whilst each having
 * wildly different characteristics. The `GET` is idempotent and has a tight latency
 * distribution while the `POST` is not idempotent and has a wide latency
 * distribution. If users want different configurations, without `MethodBuilder`
 * they must create separate Finagle clients for each grouping. While long-lived
 * clients in Finagle are not expensive, they are not free. They create
 * duplicate metrics and waste heap, file descriptors, and CPU.
 *
 * = Example =
 *
 * A client that has timeouts and retries on a 418 status code.
 * {{{
 * import com.twitter.conversions.DurationOps._
 * import com.twitter.finagle.Http
 * import com.twitter.finagle.service.{ReqRep, ResponseClass}
 * import com.twitter.util.Return
 *
 * val client: Http.Client = ???
 * client.methodBuilder("inet!example.com:80")
 *   .withTimeoutPerRequest(50.milliseconds)
 *   .withTimeoutTotal(100.milliseconds)
 *   .withRetryForClassifier {
 *     case ReqRep(_, Return(rep)) if rep.statusCode == 418 => ResponseClass.RetryableFailure
 *   }
 *   .newService("an_endpoint_name")
 * }}}
 *
 * = Timeouts =
 *
 * Defaults to using the StackClient's configuration.
 *
 * An example of setting a per-request timeout of 50 milliseconds and a total
 * timeout of 100 milliseconds:
 * {{{
 * import com.twitter.conversions.DurationOps._
 * import com.twitter.finagle.Http
 * import com.twitter.finagle.http.MethodBuilder
 *
 * val builder: MethodBuilder = ???
 * builder
 *   .withTimeoutPerRequest(50.milliseconds)
 *   .withTimeoutTotal(100.milliseconds)
 * }}}
 *
 * = Retries =
 *
 * Retries are intended to help clients improve success rate by trying
 * failed requests additional times. Care must be taken by developers
 * to only retry when it is known to be safe to issue the request multiple
 * times. This is because the client cannot always be sure what the
 * backend service has done. An example of a request that is safe to
 * retry would be a read-only request.
 *
 * Defaults to using the client's [[ResponseClassifier]] to retry failures
 * [[com.twitter.finagle.service.ResponseClass.RetryableFailure marked as retryable]].
 * See [[withRetryForClassifier]] for details.
 *
 * A [[com.twitter.finagle.service.RetryBudget]] is used to prevent retries from overwhelming
 * the backend service. The budget is shared across clients created from
 * an initial `MethodBuilder`. As such, even if the retry rules
 * deem the request retryable, it may not be retried if there is insufficient
 * budget.
 *
 * Finagle will automatically retry failures that are known to be safe
 * to retry via [[com.twitter.finagle.service.RequeueFilter]]. This includes
 * [[com.twitter.finagle.WriteException WriteExceptions]] and
 * [[com.twitter.finagle.FailureFlags.Retryable retryable nacks]]. As these should have
 * already been retried, we avoid retrying them again by ignoring them at this layer.
 *
 * Additional information regarding retries can be found in the
 * [[https://twitter.github.io/finagle/guide/Clients.html#retries user guide]].
 *
 * The classifier is also used to determine the logical success metrics of
 * the method. Logical here means after any retries are run. For example
 * should a request result in retryable failure on the first attempt, but
 * succeed upon retry, this is exposed through metrics as a success.
 * Logical success rate metrics are scoped to
 * "clnt/your_client_label/method_name/logical" and get "success" and
 * "requests" counters along with a "request_latency_ms" stat.
 *
 * Unsuccessful requests are logged at `com.twitter.logging.Level.DEBUG` level.
 * Further details, including the request and response, are available at
 * `TRACE` level.
 *
 * @see [[com.twitter.finagle.Http.Client.methodBuilder]] to construct instances.
 *
 * @see The [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]].
 */
class MethodBuilder private (mb: client.MethodBuilder[Request, Response])
    extends client.BaseMethodBuilder[MethodBuilder] {

  def withTimeoutTotal(howLong: Duration): MethodBuilder =
    new MethodBuilder(mb.withTimeout.total(howLong))

  def withTimeoutTotal(howLong: Tunable[Duration]): MethodBuilder =
    new MethodBuilder(mb.withTimeout.total(howLong))

  def withTimeoutPerRequest(howLong: Duration): MethodBuilder =
    new MethodBuilder(mb.withTimeout.perRequest(howLong))

  def withTimeoutPerRequest(howLong: Tunable[Duration]): MethodBuilder =
    new MethodBuilder(mb.withTimeout.perRequest(howLong))

  def withTraceInitializer(initializer: Filter.TypeAgnostic): MethodBuilder =
    new MethodBuilder(mb.withTraceInitializer(initializer))

  def withRetryForClassifier(classifier: ResponseClassifier): MethodBuilder =
    new MethodBuilder(mb.withRetry.forClassifier(classifier))

  def withMaxRetries(value: Int): MethodBuilder =
    new MethodBuilder(mb.withRetry.maxRetries(value))

  def withRetryDisabled: MethodBuilder =
    new MethodBuilder(mb.withRetry.disabled)

  /**
   * @inheritdoc
   *
   * This additionally causes any server error HTTP status codes (500s) to be retried.
   */
  def idempotent(maxExtraLoad: Double): MethodBuilder =
    new MethodBuilder(
      mb.idempotent(
        maxExtraLoad,
        sendInterrupts = false,
        HttpResponseClassifier.ServerErrorsAsFailures
      )
    )

  def idempotent(maxExtraLoad: Double, minSendBackupAfterMs: Int): MethodBuilder =
    new MethodBuilder(
      mb.idempotent(
        maxExtraLoad,
        sendInterrupts = false,
        minSendBackupAfterMs,
        HttpResponseClassifier.ServerErrorsAsFailures
      )
    )

  /**
   * @inheritdoc
   *
   * This additionally causes any server error HTTP status codes (500s) to be retried.
   */
  def idempotent(maxExtraLoad: Tunable[Double]): MethodBuilder =
    new MethodBuilder(
      mb.idempotent(
        maxExtraLoad,
        sendInterrupts = false,
        HttpResponseClassifier.ServerErrorsAsFailures
      )
    )

  def idempotent(maxExtraLoad: Tunable[Double], minSendBackupAfterMs: Int): MethodBuilder =
    new MethodBuilder(
      mb.idempotent(
        maxExtraLoad,
        sendInterrupts = false,
        minSendBackupAfterMs,
        HttpResponseClassifier.ServerErrorsAsFailures
      )
    )

  def nonIdempotent: MethodBuilder =
    new MethodBuilder(mb.nonIdempotent)

  /**
   * Construct a [[Service]] to be used for the `methodName` method.
   *
   * @param methodName used for scoping metrics (e.g. "clnt/your_client_label/method_name").
   */
  def newService(methodName: String): Service[Request, Response] =
    mb.newService(methodName)

  /**
   * Construct a [[Service]] to be used for the client.
   */
  def newService: Service[Request, Response] =
    mb.newService

  override def toString: String = mb.toString
}
