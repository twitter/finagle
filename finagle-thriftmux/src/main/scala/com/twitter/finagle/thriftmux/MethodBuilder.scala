package com.twitter.finagle.thriftmux

import com.twitter.finagle.thrift.{ServiceIfaceBuilder, ThriftClientRequest, ThriftServiceIface}
import com.twitter.finagle._
import com.twitter.finagle.client.RefcountedClosable
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.util.Duration

/**
 * '''Experimental:''' This API is under construction.
 */
private[finagle] object MethodBuilder {
  import client.MethodBuilder._

  /**
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newServiceIface]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[http://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def from(
    dest: String,
    thriftMuxClient: ThriftMux.Client
  ): MethodBuilder =
    from(Resolver.eval(dest), thriftMuxClient)

  /**
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newServiceIface]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[http://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def from(
    dest: Name,
    thriftMuxClient: ThriftMux.Client
  ): MethodBuilder = {
    val stack = modifiedStack(thriftMuxClient.stack)
    val params = thriftMuxClient.params
    val service: Service[ThriftClientRequest, Array[Byte]] = thriftMuxClient
      .withStack(stack)
      .newService(dest, param.Label.Default)
    val mb = new client.MethodBuilder[ThriftClientRequest, Array[Byte]](
      new RefcountedClosable(service),
      dest,
      stack,
      params,
      Config.create(thriftMuxClient.stack, params))
    new MethodBuilder(thriftMuxClient, mb)
  }
}

/**
 * '''Experimental:''' This API is under construction.

 * @example Given an example IDL:
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
 * import com.twitter.conversions.time._
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
 * @see [[com.twitter.finagle.ThriftMux.Client.methodBuilder]] to construct instances.
 *
 * @see [[client.MethodBuilderScaladoc]] for documentation.
 */
private[finagle] class MethodBuilder(
    rich: ThriftRichClient,
    mb: client.MethodBuilder[ThriftClientRequest, Array[Byte]])
  extends client.MethodBuilderScaladoc[MethodBuilder] {

  def withTimeoutTotal(howLong: Duration): MethodBuilder =
    new MethodBuilder(rich, mb.withTimeout.total(howLong))

  def withTimeoutPerRequest(howLong: Duration): MethodBuilder =
    new MethodBuilder(rich, mb.withTimeout.perRequest(howLong))

  def withRetryForClassifier(classifier: ResponseClassifier): MethodBuilder =
    new MethodBuilder(rich, mb.withRetry.forClassifier(classifier))

  def withRetryDisabled: MethodBuilder =
    new MethodBuilder(rich, mb.withRetry.disabled)

  /**
   * Construct a `ServiceIface` to be used for the `methodName` function.
   *
   * @param methodName used for scoping metrics (e.g. "clnt/your_client_label/method_name").
   */
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
    methodName: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val filters: Filter.TypeAgnostic = mb.filters(methodName)
    val serviceIface: ServiceIface = rich.newServiceIface(
      mb.wrappedService(methodName),
      mb.params[param.Label].label
    )(builder)
    serviceIface.filtered(filters)
  }
}
