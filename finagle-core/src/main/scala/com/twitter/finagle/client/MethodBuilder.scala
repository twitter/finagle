package com.twitter.finagle.client

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.client.MethodBuilderTimeout.TunableDuration
import com.twitter.finagle.param.ResponseClassifier
import com.twitter.finagle.service.{Retries, TimeoutFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{Showable, StackRegistry}
import com.twitter.finagle.{Filter, Name, Service, ServiceFactory, Stack, param, _}
import com.twitter.util.tunable.Tunable
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
      Config.create(stackClient.stack, stackClient.params)
    )
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
          stackTotalTimeoutDefined = originalStack.contains(TimeoutFilter.totalTimeoutRole),
          total = TunableDuration(id = "total", duration = params[TimeoutFilter.TotalTimeout].timeout),
          perRequest = TunableDuration(id = "perRequest", duration = params[TimeoutFilter.Param].timeout)
        )
      )
    }
  }

  /**
   * @see [[MethodBuilder.Config.create]] to construct an initial instance.
   *       Using its `copy` method is appropriate after that.
   */
  case class Config private (retry: MethodBuilderRetry.Config, timeout: MethodBuilderTimeout.Config)

  /** Used by the `ClientRegistry` */
  private[client] val RegistryKey = "methods"

}

/**
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
  private[client] val config: MethodBuilder.Config
) { self =>
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


  /**
   * Configure that requests are to be treated as idempotent. Because requests can be safely
   * retried, [[BackupRequestFilter]] is configured with the params `maxExtraLoad` and
   * `sendInterrupts` to decrease tail latency by sending an additional fraction of requests.
   *
   * @param maxExtraLoad How much extra load, as a fraction, we are willing to send to the server.
   *                     Must be between 0.0 and 1.0.
   * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
   *                       is returned and the result of the outstanding request is superseded. For
   *                       protocols without a control plane, where the connection is cut on
   *                       interrupts, this should be "false" to avoid connection churn.
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retries.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
   *
   * @note See `idempotent` below for a version that takes a [[Tunable[Double]]] for `maxExtraLoad`.
   */
  def idempotent(
    maxExtraLoad: Double,
    sendInterrupts: Boolean,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    val brfParam =
      if (maxExtraLoad == 0) BackupRequestFilter.Disabled
      else BackupRequestFilter.Configured(maxExtraLoad, sendInterrupts)
    addBackupRequestFilterParamAndClassifier(brfParam, classifier)
  }

  /**
   * Configure that requests are to be treated as idempotent. Because requests can be safely
   * retried, [[BackupRequestFilter]] is configured with the params `maxExtraLoad` and
   * `sendInterrupts` to decrease tail latency by sending an additional fraction of requests.
   *
   * If you are using TwitterServer, a good starting point for determining a value for
   * `maxExtraLoad` is looking at the details of the PDF histogram for request latency,
   * at /admin/histograms. If you choose a `maxExtraLoad` of 1.percent, for example, you can expect
   * your p999/p9999 latencies to (roughly) now be that of your p99 latency. For 5.percent, those
   * latencies would shift to your p95 latency. You should also ensure that your backend can
   * tolerate the increased load.
   *
   * @param maxExtraLoad How much extra load, as a [[Tunable]] fraction, we are willing to send to
   *                     the server. Must be between 0.0 and 1.0.
   * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
   *                       is returned and the result of the outstanding request is superseded. For
   *                       protocols without a control plane, where the connection is cut on
   *                       interrupts, this should be "false" to avoid connection churn.
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retries.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
   */
  def idempotent(
    maxExtraLoad: Tunable[Double],
    sendInterrupts: Boolean,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    addBackupRequestFilterParamAndClassifier(
      BackupRequestFilter.Configured(maxExtraLoad, sendInterrupts), classifier)
  }

  private[this] def addBackupRequestFilterParamAndClassifier(
    brfParam: BackupRequestFilter.Param,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    val combinedClassifier =
      if (!params.contains[ResponseClassifier]) classifier
      else (params[ResponseClassifier].responseClassifier).orElse(classifier)
    (new MethodBuilder[Req, Rep](
      refCounted,
      dest,
      stack,
      // If the RetryBudget is not configured, BackupRequestFilter and RetryFilter will each
      // get a new instance of the default budget. Since we want them to share the same
      // client retry budget, insert the budget into the params.
      stackParams + brfParam + ResponseClassifier(combinedClassifier) + stackParams[Retries.Budget],
      config))
      .withRetry.forClassifier(combinedClassifier)
  }

  /**
   * Configure that requests are to be treated as non-idempotent. [[BackupRequestFilter]] is
   * disabled, and only those failures that are known to be safe to retry (i.e., write failures,
   * where the request was never sent) are retried via requeue filter; any previously configured
   * retries are removed.
   */
  def nonIdempotent: MethodBuilder[Req, Rep] = {
    new MethodBuilder[Req, Rep](
      refCounted,
      dest,
      stack,
      stackParams + BackupRequestFilter.Disabled,
      config)
    .withRetry.forClassifier(service.ResponseClassifier.Default)
  }

  //
  // Build
  //

  private[this] def newService(methodName: Option[String]): Service[Req, Rep] =
    filters(methodName).andThen(wrappedService(methodName))

  /**
   * Create a [[Service]] from the current configuration.
   */
  def newService(methodName: String): Service[Req, Rep] =
    newService(Some(methodName))

  /**
   * Create a [[Service]] from the current configuration.
   */
  def newService: Service[Req, Rep] =
    newService(None)

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
    new MethodBuilder(refCounted, dest, stack, stackParams, config)

  private[this] def clientName: String =
    stackParams[param.Label].label match {
      case param.Label.Default => Showable.show(dest)
      case label => label
    }

  private[this] def statsReceiver(methodName: Option[String]): StatsReceiver = {
    val clientScoped = stackParams[param.Stats].statsReceiver.scope(clientName)
    methodName match {
      case Some(methodName) => clientScoped.scope(methodName)
      case None => clientScoped
    }
  }


  def filters(methodName: Option[String]): Filter.TypeAgnostic = {
    // Ordering of filters:
    // Requests start at the top and traverse down.
    // Responses flow back from the bottom up.
    //
    // Backups are positioned after retries to avoid request amplification of retries, after
    // total timeouts and before per-request timeouts so that each backup uses the per-request
    // timeout.
    //
    // - Logical Stats
    // - Failure logging
    // - Annotate method name for a `Failure`
    // - Total Timeout
    // - Retries
    // - Backup Requests
    // - Service (Finagle client's stack, including Per Request Timeout)

    val stats = statsReceiver(methodName)
    val retries = withRetry
    val timeouts = withTimeout

    val failureSource = methodName match {
      case Some(methodName) => addFailureSource(methodName)
      case None => TypeAgnostic.Identity
    }

    retries
      .logicalStatsFilter(stats)
      .andThen(retries.logFailuresFilter(clientName, methodName))
      .andThen(failureSource)
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

  private[this] def registryKeyPrefix(methodName: Option[String]): Seq[String] =
    methodName match {
      case Some(name) => Seq(RegistryKey, name)
      case None => Seq(RegistryKey)
    }

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
  private[this] def addToRegistry(name: Option[String]): Unit = {
    val entry = registryEntry()
    val keyPrefix = registryKeyPrefix(name)
    ClientRegistry.register(entry, keyPrefix :+ "statsReceiver", statsReceiver(name).toString)
    withTimeout.registryEntries.foreach {
      case (suffix, value) =>
        ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
    withRetry.registryEntries.foreach {
      case (suffix, value) =>
        ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
  }

  def wrappedService(name: Option[String]): Service[Req, Rep] = {
    addToRegistry(name)
    refCounted.open()

    val underlying = BackupRequestFilter.filterService(
      stackParams + param.Stats(statsReceiver(name)), refCounted.get)

    new ServiceProxy[Req, Rep](underlying) {
      private[this] val isClosed = new AtomicBoolean(false)
      private[this] val closedP = new Promise[Unit]()

      override def apply(request: Req): Future[Rep] =
        if (isClosed.get) Future.exception(new ServiceClosedException())
        else super.apply(request)

      override def status: Status =
        if (isClosed.get) Status.Closed
        else underlying.status

      override def close(deadline: Time): Future[Unit] = {
        if (isClosed.compareAndSet(false, true)) {
          // remove our method builder's entries from the registry
          ClientRegistry.unregisterPrefixes(registryEntry(), registryKeyPrefix(name))
          // call refCounted.close to decrease the ref count. `underlying.close` is only
          // called when the closable underlying `refCounted` is closed.
          closedP.become(refCounted.close(deadline).transform(_ => underlying.close(deadline)))
        }
        closedP
      }
    }
  }

}
