package com.twitter.finagle.http

import com.twitter.{finagle => ctf}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.{Name, Resolver, Service, client}
import com.twitter.util.Duration

/**
 * '''Experimental:''' This API is under construction.
 */
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
  def from(
    dest: String,
    stackClient: StackClient[Request, Response]
  ): MethodBuilder =
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
  def from(
    dest: Name,
    stackClient: StackClient[Request, Response]
  ): MethodBuilder = {
    val mb = client.MethodBuilder.from(dest, stackClient)
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
 * '''Experimental:''' This API is under construction.
 *
 * @example A client that has timeouts and retries on a 418 status code.
 * {{{
 * import com.twitter.conversions.time._
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
 * @see [[com.twitter.finagle.Http.Client.methodBuilder]] to construct instances.
 *
 * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
 *      and [[client.MethodBuilderScaladoc]] for documentation.
 */
class MethodBuilder private (
    mb: client.MethodBuilder[Request, Response])
  extends client.MethodBuilderScaladoc[MethodBuilder] {

  def withTimeoutTotal(howLong: Duration): MethodBuilder =
    new MethodBuilder(mb.withTimeout.total(howLong))

  def withTimeoutPerRequest(howLong: Duration): MethodBuilder =
    new MethodBuilder(mb.withTimeout.perRequest(howLong))

  def withRetryForClassifier(classifier: ResponseClassifier): MethodBuilder =
    new MethodBuilder(mb.withRetry.forClassifier(classifier))

  def withRetryDisabled: MethodBuilder =
    new MethodBuilder(mb.withRetry.disabled)

  /**
   * Construct a [[Service]] to be used for the `methodName` method.
   *
   * @param methodName used for scoping metrics (e.g. "clnt/your_client_label/method_name").
   */
  def newService(methodName: String): Service[Request, Response] =
    mb.newService(methodName)

}
