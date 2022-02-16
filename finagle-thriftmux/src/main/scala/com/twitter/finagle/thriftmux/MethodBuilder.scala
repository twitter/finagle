package com.twitter.finagle.thriftmux

import com.twitter.finagle.client
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.builder.ClientConfig
import com.twitter.finagle.client.MethodPool
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService
import com.twitter.finagle.thrift.service.Filterable
import com.twitter.finagle.thrift.service.ServicePerEndpointBuilder
import com.twitter.finagle.thrift.ServiceIfaceBuilder
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.thriftmux.exp.partitioning.DynamicPartitioningService
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.tunable.Tunable

object MethodBuilder {
  import client.MethodBuilder._

  /**
   * Create a [[MethodBuilder]] for a given destination.
   *
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.servicePerEndpoint]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   *
   * @see [[com.twitter.finagle.ThriftMux.Client.methodBuilder(String)]]
   */
  def from(dest: String, thriftMuxClient: ThriftMux.Client): MethodBuilder =
    from(Resolver.eval(dest), thriftMuxClient)

  /**
   * Create a [[MethodBuilder]] for a given destination.
   *
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.servicePerEndpoint]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   *
   * @see [[com.twitter.finagle.ThriftMux.Client.methodBuilder(Name)]]
   */
  def from(dest: Name, thriftMuxClient: ThriftMux.Client): MethodBuilder = {
    val stack = modifiedStack(thriftMuxClient.stack)
      .replace(ThriftPartitioningService.role, DynamicPartitioningService.perRequestModule)
    val params = thriftMuxClient.params

    val mb = new client.MethodBuilder[ThriftClientRequest, Array[Byte]](
      new MethodPool[ThriftClientRequest, Array[Byte]](
        thriftMuxClient.withStack(stack),
        dest,
        param.Label.Default),
      dest,
      stack,
      params,
      Config.create(thriftMuxClient.stack, params)
    )
    new MethodBuilder(thriftMuxClient, mb)
  }

  /**
   * '''NOTE:''' Prefer using [[com.twitter.finagle.ThriftMux.Client.methodBuilder]] over using
   * this approach to construction. The functionality is available through
   * [[ThriftMux.Client]] and [[MethodBuilder]] while addressing the various issues
   * of `ClientBuilder`.
   *
   * Creates a [[MethodBuilder]] from the given [[ClientBuilder]].
   *
   * Note that metrics will be scoped (e.g. "clnt/clientbuilders_name/method_name").
   *
   * The value for "clientbuilders_name" is taken from the [[ClientBuilder.name]]
   * configuration, using "client" if unspecified.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.servicePerEndpoint]].
   *
   *  - The [[ClientBuilder.timeout]] configuration will be used as the default
   *  value for [[MethodBuilder.withTimeoutTotal]].
   *
   *  - The [[ClientBuilder.requestTimeout]] configuration will be used as the
   *  default value for [[MethodBuilder.withTimeoutPerRequest]].
   *
   *  - The [[ClientBuilder]] must have been constructed using
   *  [[ClientBuilder.stack]] passing an instance of a [[ThriftMux.Client]].
   *
   *  - The [[ClientBuilder]] metrics scoped to "tries" are not included
   *  as they are superseded by metrics scoped to "logical".
   *
   *  - The [[ClientBuilder]] retry policy will not be applied and must
   *  be migrated to using [[MethodBuilder.withRetryForClassifier]].
   *
   *  - The [[ClientBuilder]] retries will not be applied and must
   *  be migrated to using [[MethodBuilder.withMaxRetries]].
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#migrating-from-clientbuilder user guide]]
   */
  def from(
    clientBuilder: ClientBuilder[ThriftClientRequest, Array[Byte], ClientConfig.Yes, _, _]
  ): MethodBuilder = {
    if (!clientBuilder.params.contains[ClientConfig.DestName])
      throw new IllegalArgumentException("ClientBuilder must be configured with a dest")
    val dest = clientBuilder.params[ClientConfig.DestName].name
    val client = clientBuilder.client.asInstanceOf[ThriftMux.Client]
    from(dest, client)
  }

}

/**
 * `MethodBuilder` is a collection of APIs for client configuration at
 * a higher level than the Finagle 6 APIs while improving upon the deprecated
 * [[ClientBuilder]]. `MethodBuilder` provides:
 *
 *  - Logical success rate metrics.
 *  - Retries based on application-level requests and responses (e.g. a code in
 *    the Thrift response).
 *  - Configuration of per-attempt and total timeouts.
 *
 * All of these can be customized per method (or endpoint) while sharing a single
 * underlying Finagle client. Concretely, a single service might offer both
 * `getOneTweet` as well as `deleteTweets`, whilst each having
 * wildly different characteristics. The get is idempotent and has a tight latency
 * distribution while the delete is not idempotent and has a wide latency
 * distribution. If users want different configurations, without `MethodBuilder`
 * they must create separate Finagle clients for each grouping. While long-lived
 * clients in Finagle are not expensive, they are not free. They create
 * duplicate metrics and waste heap, file descriptors, and CPU.
 *
 * = Example =
 *
 * Given an example IDL:
 * {{{
 * exception AnException {
 *   1: i32 errorCode
 * }
 *
 * service SomeService {
 *   i32 TheMethod(
 *     1: i32 input
 *   ) throws (
 *     1: AnException ex1,
 *   )
 * }
 * }}}
 *
 * This gives you a `Service` that has timeouts and retries on
 * `AnException` when the `errorCode` is `0`:
 * {{{
 * import com.twitter.conversions.DurationOps._
 * import com.twitter.finagle.ThriftMux
 * import com.twitter.finagle.service.{ReqRep, ResponseClass}
 * import com.twitter.util.Throw
 *
 * val client: ThriftMux.Client = ???
 * val svc: Service[TheMethod.Args, TheMethod.SuccessType] =
 *   client.methodBuilder("inet!example.com:5555")
 *     .withTimeoutPerRequest(50.milliseconds)
 *     .withTimeoutTotal(100.milliseconds)
 *     .withRetryForClassifier {
 *       case ReqRep(_, Throw(AnException(errCode))) if errCode == 0 =>
 *         ResponseClass.RetryableFailure
 *     }
 *     .newServiceIface("the_method")
 *     .theMethod
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
 * import com.twitter.finagle.thriftmux.MethodBuilder
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
 * An example of configuring classifiers for ChannelClosed and Timeout exceptions:
 * {{{
 * import com.twitter.finagle.service.ResponseClassifier._
 * import com.twitter.finagle.thriftmux.MethodBuilder
 *
 * val builder: MethodBuilder = ???
 * builder
 *   .withRetryForClassifier(RetryOnChannelClosed.orElse(RetryOnTimeout))
 * }}}
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
 * @see [[com.twitter.finagle.ThriftMux.Client.methodBuilder]] to construct instances.
 *
 * @see The [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]].
 */
class MethodBuilder(
  thriftMuxClient: ThriftMux.Client,
  mb: client.MethodBuilder[ThriftClientRequest, Array[Byte]])
    extends client.BaseMethodBuilder[MethodBuilder] {

  /**
   * Configured client label. The `label` is used to assign a label to the underlying Thrift client.
   * The label is used to display stats, etc.
   *
   * @see [[com.twitter.finagle.Client]]
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#clients]]
   */
  def label: String = mb.params[param.Label].label

  def withTimeoutTotal(howLong: Duration): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withTimeout.total(howLong))

  def withTimeoutTotal(howLong: Tunable[Duration]): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withTimeout.total(howLong))

  def withTimeoutPerRequest(howLong: Duration): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withTimeout.perRequest(howLong))

  def withTimeoutPerRequest(howLong: Tunable[Duration]): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withTimeout.perRequest(howLong))

  def withRetryForClassifier(classifier: ResponseClassifier): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withRetry.forClassifier(classifier))

  def withMaxRetries(value: Int): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withRetry.maxRetries(value))

  def withRetryDisabled: MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.withRetry.disabled)

  /**
   * Set a [[PartitioningStrategy]] for a MethodBuilder endpoint to enable
   * partitioning awareness. See [[PartitioningStrategy]].
   *
   * Default is [[com.twitter.finagle.thrift.exp.partitioning.Disabled]]
   *
   * @example
   * To set a hashing strategy to MethodBuilder:
   * {{{
   * import com.twitter.finagle.ThriftMux.Client
   * import com.twitter.finagle.thrift.exp.partitioning.MethodBuilderHashingStrategy
   *
   * val hashingStrategy = new MethodBuilderHashingStrategy[RequestType, ResponseType](...)
   *
   * val client: ThriftMux.Client = ???
   * val builder = client.methodBuilder($address)
   *
   * builder
   *   .withPartitioningStrategy(hashingStrategy)
   *   .servicePerEndpoint...
   * ...
   * }}}
   */
  def withPartitioningStrategy(strategy: PartitioningStrategy): MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.filtered(partitioningFilter(strategy)))

  private[this] def partitioningFilter(
    partitionStrategy: PartitioningStrategy
  ): Filter.TypeAgnostic = {
    new Filter.TypeAgnostic {
      def toFilter[Req, Rep] = new SimpleFilter[Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          DynamicPartitioningService.letStrategy(partitionStrategy) {
            service(request)
          }
        }
      }
    }
  }

  /**
   * @inheritdoc
   *
   * This additionally causes Thrift Exceptions to be retried.
   */
  def idempotent(maxExtraLoad: Double): MethodBuilder =
    new MethodBuilder(
      thriftMuxClient,
      mb.idempotent(maxExtraLoad, sendInterrupts = true, ResponseClassifier.RetryOnThrows)
    )

  /**
   * @inheritdoc
   *
   * This additionally causes Thrift Exceptions to be retried.
   */
  def idempotent(maxExtraLoad: Tunable[Double]): MethodBuilder =
    new MethodBuilder(
      thriftMuxClient,
      mb.idempotent(maxExtraLoad, sendInterrupts = true, ResponseClassifier.RetryOnThrows)
    )

  def nonIdempotent: MethodBuilder =
    new MethodBuilder(thriftMuxClient, mb.nonIdempotent)

  /**
   * Construct a `ServiceIface` to be used for the `methodName` function.
   *
   * @param methodName used for scoping metrics (e.g. "clnt/your_client_label/method_name").
   */
  @deprecated("Use servicePerEndpoint", "2017-11-29")
  def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
    methodName: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val clientBuilder = new ClientServiceIfaceBuilder[ServiceIface](builder)

    mb.configured(Thrift.param.ServiceClass(Option(clientBuilder.serviceClass)))
      .newServicePerEndpoint(clientBuilder, methodName)
  }

  /**
   * Construct a `ServicePerEndpoint` to be used for the `methodName` function.
   *
   * @param methodName used for scoping metrics (e.g. "clnt/your_client_label/method_name").
   */
  def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    methodName: String
  )(
    implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
  ): ServicePerEndpoint = {
    val clientBuilder = new ClientServicePerEndpointBuilder[ServicePerEndpoint](builder)

    mb.configured(Thrift.param.ServiceClass(Option(clientBuilder.serviceClass)))
      .newServicePerEndpoint(clientBuilder, methodName).getServicePerEndpoint
  }

  /**
   * Construct a `ServicePerEndpoint` to be used for the client.
   */
  def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
  ): ServicePerEndpoint = {
    val clientBuilder = new ClientServicePerEndpointBuilder[ServicePerEndpoint](builder)

    mb.configured(Thrift.param.ServiceClass(Option(builder.serviceClass)))
      .newServicePerEndpoint(clientBuilder).getServicePerEndpoint
  }

  /**
   * Create a [[Service]] from the current configuration.
   *
   * @note It's very likely that you wanted/needed to use {{servicePerEndpoint}} instead.
   */
  def newService(methodName: String): Service[ThriftClientRequest, Array[Byte]] =
    mb.newService(methodName)

  /**
   * Create a [[Service]] from the current configuration.
   *
   * @note It's very likely that you wanted/needed to use {{servicePerEndpoint}} instead.
   */
  def newService: Service[ThriftClientRequest, Array[Byte]] =
    mb.newService

  final private class ClientServiceIfaceBuilder[ServiceIface <: Filterable[ServiceIface]](
    builder: ServiceIfaceBuilder[ServiceIface])
      extends client.ServicePerEndpointBuilder[
        ThriftClientRequest,
        Array[Byte],
        ServiceIface
      ] {
    override def servicePerEndpoint(
      service: => Service[ThriftClientRequest, Array[Byte]]
    ): ServiceIface = thriftMuxClient.newServiceIface(service, label)(builder)

    override def serviceClass: Class[_] = builder.serviceClass
  }

  final private class ClientServicePerEndpointBuilder[
    ServicePerEndpoint <: Filterable[ServicePerEndpoint]
  ](
    builder: ServicePerEndpointBuilder[ServicePerEndpoint])
      extends client.ServicePerEndpointBuilder[
        ThriftClientRequest,
        Array[Byte],
        DelayedTypeAgnosticFilterable[ServicePerEndpoint]
      ] {

    override def servicePerEndpoint(
      service: => Service[ThriftClientRequest, Array[Byte]]
    ): DelayedTypeAgnosticFilterable[ServicePerEndpoint] = {
      new DelayedTypeAgnosticFilterable(
        thriftMuxClient.servicePerEndpoint(
          new DelayedService(service),
          label
        )(builder)
      )
    }

    override def serviceClass: Class[_] = builder.serviceClass
  }

  // used to delay creation of the Service until the first request
  // as `mb.wrappedService` eagerly creates some metrics that are best
  // avoided until the first request.
  final private class DelayedService(service: => Service[ThriftClientRequest, Array[Byte]])
      extends Service[ThriftClientRequest, Array[Byte]] {
    private[this] lazy val svc: Service[ThriftClientRequest, Array[Byte]] =
      service

    def apply(request: ThriftClientRequest): Future[Array[Byte]] =
      svc(request)

    override def close(deadline: Time): Future[Unit] =
      svc.close(deadline)

    override def status: Status =
      svc.status
  }

  // A filterable that wraps each filter with DelayedTypeAgnostic before applying
  // it to the underlying servicePerEndpoint
  final private class DelayedTypeAgnosticFilterable[T <: Filterable[T]](servicePerEndpoint: T)
      extends Filterable[DelayedTypeAgnosticFilterable[T]] {

    def getServicePerEndpoint: T = servicePerEndpoint

    override def filtered(filter: Filter.TypeAgnostic): DelayedTypeAgnosticFilterable[T] =
      new DelayedTypeAgnosticFilterable[T](
        servicePerEndpoint.filtered(new DelayedTypeAgnostic(filter))
      )
  }

  // used to delay creation of the Filters until the first request
  // as `servicePerEndpoint.filtered` eagerly creates some metrics
  // that are best avoided until the first request.
  final private class DelayedTypeAgnostic(typeAgnostic: Filter.TypeAgnostic)
      extends Filter.TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new Filter[Req, Rep, Req, Rep] {
      private lazy val filter: Filter[Req, Rep, Req, Rep] =
        typeAgnostic.toFilter

      def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
        filter(request, service)
    }
  }

  override def toString: String = mb.toString
}
