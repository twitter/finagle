package com.twitter.finagle.client

import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{Showable, StackRegistry}
import com.twitter.finagle.{Filter, Name, Service, ServiceFactory, Stack, param, _}
import com.twitter.util.{Future, Promise, Time}
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] object MethodBuilder {

  /**
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newService(String)]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def from[Req, Rep](
    dest: String,
    stackClient: StackClient[Req, Rep]
  ): MethodBuilder[Req, Rep] =
    from(Resolver.eval(dest), stackClient)

  /**
   * Note that metrics will be scoped (e.g. "clnt/your_client_label/method_name").
   *
   * The value for "your_client_label" is taken from the `withLabel` setting
   * (from [[param.Label]]). If that is not set, `dest` is used.
   * The value for "method_name" is set when an method-specific client
   * is constructed, as in [[MethodBuilder.newService(String)]].
   *
   * @param dest where requests are dispatched to.
   *             See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def from[Req, Rep](
    dest: Name,
    stackClient: StackClient[Req, Rep]
  ): MethodBuilder[Req, Rep] = {
    val stack = modifiedStack(stackClient.stack)
    val service: Service[Req, Rep] = stackClient
      .withStack(stack)
      .newService(dest, param.Label.Default)
    new MethodBuilder(
      new RefcountedClosable(service),
      dest,
      stack,
      stackClient.params,
      Config.create(stackClient.stack, stackClient.params))
  }

  /**
   * Modifies the given [[Stack]] so that it is ready for use
   * in a [[MethodBuilder]] client.
   */
  def modifiedStack[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]]
  ): Stack[ServiceFactory[Req, Rep]] = {
    stack
      // total timeouts are managed directly by MethodBuilder
      .remove(TimeoutFilter.totalTimeoutRole)
      // allow for dynamic per-request timeouts
      .replace(TimeoutFilter.role, DynamicTimeout.perRequestModule[Req, Rep])
  }

  object Config {
    /**
     * @param originalStack the `Stack` before [[modifiedStack]] was called.
     */
    def create(
      originalStack: Stack[_],
      params: Stack.Params
    ): Config = {
      Config(
        MethodBuilderRetry.Config(params[param.ResponseClassifier].responseClassifier),
        MethodBuilderTimeout.Config(
          stackHadTotalTimeout = originalStack.contains(TimeoutFilter.totalTimeoutRole),
          total = params[TimeoutFilter.TotalTimeout].timeout,
          perRequest = params[TimeoutFilter.Param].timeout)
      )
    }
  }

  /**
   * @see [[MethodBuilder.Config.create]] to construct an initial instance.
   *       Using its `copy` method is appropriate after that.
   */
  case class Config private (
      retry: MethodBuilderRetry.Config,
      timeout: MethodBuilderTimeout.Config)

  /** Used by the `ClientRegistry` */
  private[client] val RegistryKey = "methods"

}

/**
 * '''Experimental:''' This API is under construction.
 *
 * @see `methodBuilder` methods on client protocols, such as `Http.Client`
 *      or `ThriftMux.Client` for an entry point.
 *
 * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
 */
private[finagle] final class MethodBuilder[Req, Rep](
    val refCounted: RefcountedClosable[Service[Req, Rep]],
    dest: Name,
    stack: Stack[_],
    stackParams: Stack.Params,
    private[client] val config: MethodBuilder.Config) { self =>
  import MethodBuilder._

  //
  // Configuration
  //

  /**
   * Configure the application-level retry policy.
   *
   * Defaults to using the client's [[com.twitter.finagle.service.ResponseClassifier]]
   * to retry failures
   * [[com.twitter.finagle.service.ResponseClass.RetryableFailure marked as retryable]].
   *
   * The classifier is also used to determine the logical success metrics of
   * the client. Logical here means after any retries are run. For example
   * should a request result in retryable failure on the first attempt, but
   * succeed upon retry, this is exposed through metrics as a success.
   *
   * @example Retrying on `Exception` responses:
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
   * @see [[MethodBuilderRetry]]
   */
  def withRetry: MethodBuilderRetry[Req, Rep] =
    new MethodBuilderRetry[Req, Rep](this)

  /**
   * Configure the timeouts.
   *
   * The per-request timeout defaults to using the client's configuration for
   * [[com.twitter.finagle.service.TimeoutFilter.Param(timeout)]],
   * which is typically set via
   * [[com.twitter.finagle.param.CommonParams.withRequestTimeout]].
   *
   * The total timeout defaults to using the client's configuration for
   * [[com.twitter.finagle.service.TimeoutFilter.TotalTimeout(timeout)]].
   *
   * @example A total timeout of 200 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.client.MethodBuilder
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withTimeout.total(200.milliseconds)
   * }}}
   *
   * @example A per-request timeout of 50 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.client.MethodBuilder
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withTimeout.perRequest(50.milliseconds)
   * }}}
   *
   * @see [[MethodBuilderTimeout]]
   */
  def withTimeout: MethodBuilderTimeout[Req, Rep] =
    new MethodBuilderTimeout[Req, Rep](self)

  //
  // Build
  //

  /**
   * Create a [[Service]] from the current configuration.
   *
   * @param methodName used for scoping metrics
   */
  def newService(methodName: String): Service[Req, Rep] =
    filters(methodName).andThen(wrappedService(methodName))

  //
  // Internals
  //

  def params: Stack.Params =
    stackParams

  /**
   * '''For implementers'''
   *
   * Create a new instance of this [[MethodBuilder]] with the
   * `Config` modified.
   */
  private[client] def withConfig(config: Config): MethodBuilder[Req, Rep] =
    new MethodBuilder(
      refCounted,
      dest,
      stack,
      stackParams,
      config)

  private[this] def statsReceiver(name: String): StatsReceiver = {
    val clientName = stackParams[param.Label].label match {
      case param.Label.Default => Showable.show(dest)
      case label => label
    }
    stackParams[param.Stats].statsReceiver.scope(clientName, name)
  }

  def filters(methodName: String): Filter.TypeAgnostic = {
    // Ordering of filters:
    // Requests start at the top and traverse down.
    // Responses flow back from the bottom up.
    //
    // - Logical Stats
    // - Annotate method name for a `Failure`
    // - Total Timeout
    // - Retries
    // - Service (Finagle client's stack, including Per Request Timeout)

    val stats = statsReceiver(methodName)
    val retries = withRetry
    val timeouts = withTimeout

    retries.logicalStatsFilter(stats)
      .andThen(addFailureSource(methodName))
      .andThen(timeouts.totalFilter)
      .andThen(retries.filter(stats))
      .andThen(timeouts.perRequestFilter)
  }

  private[this] def addFailureSource(methodName: String) = new Filter.TypeAgnostic {
    def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = new SimpleFilter[Req1, Rep1] {
      private[this] val onRescue: PartialFunction[Throwable, Future[Rep1]] = {
        case f: Failure =>
          Future.exception(f.withSource(Failure.Source.Method, methodName))
      }

      def apply(request: Req1, service: Service[Req1, Rep1]): Future[Rep1] =
        service(request).rescue(onRescue)
    }
  }

  private[this] def registryEntry(): StackRegistry.Entry =
    StackRegistry.Entry(Showable.show(dest), stack, params)

  private[this] def registryKeyPrefix(name: String): Seq[String] =
    Seq(RegistryKey, name)

  // clients get registered at:
  // client/$protocol_lib/$client_name/$dest_addr
  //
  // methodbuilders are registered at:
  // client/$protocol_lib/$client_name/$dest_addr/methods/$method_name
  //
  // with the suffixes looking something like:
  //   stats_receiver: StatsReceiver/scope
  //   retry: DefaultResponseClassifier
  //   timeout/total: 100.milliseconds
  //   timeout/per_request: 30.milliseconds
  private[this] def addToRegisty(name: String): Unit = {
    val entry = registryEntry()
    val keyPrefix = registryKeyPrefix(name)
    ClientRegistry.register(entry, keyPrefix :+ "statsReceiver", statsReceiver(name).toString)
    withTimeout.registryEntries.foreach { case (suffix, value) =>
      ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
    withRetry.registryEntries.foreach { case (suffix, value) =>
      ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
  }

  def wrappedService(name: String): Service[Req, Rep] = {
    addToRegisty(name)
    refCounted.open()
    new ServiceProxy[Req, Rep](refCounted.get) {
      private[this] val isClosed = new AtomicBoolean(false)
      private[this] val closedP = new Promise[Unit]()

      override def apply(request: Req): Future[Rep] =
        if (isClosed.get) Future.exception(new ServiceClosedException())
        else super.apply(request)

      override def status: Status =
        if (isClosed.get) Status.Closed
        else refCounted.get.status

      override def close(deadline: Time): Future[Unit] = {
        if (isClosed.compareAndSet(false, true)) {
          // remove our method builder's entries from the registry
          ClientRegistry.unregisterPrefixes(registryEntry(), registryKeyPrefix(name))
          // and decrease the ref count
          closedP.become(refCounted.close())
        }
        closedP
      }
    }
  }

}
