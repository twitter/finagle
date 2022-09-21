package com.twitter.finagle.client

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.client.MethodBuilderTimeout.TunableDuration
import com.twitter.finagle.service.Filterable
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.Retries
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.LazyStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.util.Showable
import com.twitter.finagle.util.StackRegistry
import com.twitter.finagle.Filter
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.param
import com.twitter.finagle._
import com.twitter.util.tunable.Tunable
import com.twitter.util.Closable
import com.twitter.util.CloseOnce
import com.twitter.util.Future
import com.twitter.util.Time

object MethodBuilder {

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
  def from[Req, Rep](dest: String, stackClient: StackClient[Req, Rep]): MethodBuilder[Req, Rep] =
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
  def from[Req, Rep](dest: Name, stackClient: StackClient[Req, Rep]): MethodBuilder[Req, Rep] = {
    val stack = modifiedStack(stackClient.stack)
    new MethodBuilder(
      new MethodPool[Req, Rep](stackClient.withStack(stack), dest, param.Label.Default),
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
  private[finagle] def modifiedStack[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]]
  ): Stack[ServiceFactory[Req, Rep]] = {
    stack
    // We need to move trace initializing filter up so it "covers' MB's own
    // filters such as retries and backups.
      .remove(TraceInitializerFilter.role)
      // total timeouts are managed directly by MethodBuilder
      .remove(TimeoutFilter.totalTimeoutRole)
      // allow for dynamic per-request timeouts
      .replace(TimeoutFilter.role, DynamicTimeout.perRequestModule[Req, Rep])
      // If the stack we're working with had support for dynamic BRF, let's install the
      // real filter instead of a placeholder.
      .replace(
        DynamicBackupRequestFilter.role,
        DynamicBackupRequestFilter.perRequestModule[Req, Rep])
  }

  private[finagle] object Config {

    /**
     * @param originalStack the `Stack` before [[modifiedStack]] was called.
     */
    def create(originalStack: Stack[_], params: Stack.Params): Config = {
      Config(
        TraceInitializerFilter.typeAgnostic(params[param.Tracer].tracer, true),
        MethodBuilderRetry.Config(
          if (params.contains[param.ResponseClassifier]) {
            Some(params[param.ResponseClassifier].responseClassifier)
          } else None
        ),
        MethodBuilderTimeout.Config(
          stackTotalTimeoutDefined = originalStack.contains(TimeoutFilter.totalTimeoutRole),
          total =
            TunableDuration(id = "total", duration = params[TimeoutFilter.TotalTimeout].timeout),
          perRequest =
            TunableDuration(id = "perRequest", duration = params[TimeoutFilter.Param].timeout)
        )
      )
    }
  }

  /**
   * @see [[MethodBuilder.Config.create]] to construct an initial instance.
   *       Using its `copy` method is appropriate after that.
   */
  private[finagle] case class Config private (
    traceInitializer: Filter.TypeAgnostic,
    retry: MethodBuilderRetry.Config,
    timeout: MethodBuilderTimeout.Config,
    filter: Filter.TypeAgnostic = Filter.typeAgnosticIdentity,
    backup: BackupRequestFilter.Param = BackupRequestFilter.Param.Disabled)

  /** Used by the `ClientRegistry` */
  private[client] val RegistryKey = "methods"

}

/**
 * @see `methodBuilder` methods on client protocols, such as `Http.Client`
 *      or `ThriftMux.Client` for an entry point.
 *
 * @see [[https://twitter.github.io/finagle/guide/MethodBuilder.html user guide]]
 */
final class MethodBuilder[Req, Rep] private[finagle] (
  val methodPool: MethodPool[Req, Rep],
  dest: Name,
  stack: Stack[_],
  val params: Stack.Params,
  private[client] val config: MethodBuilder.Config)
    extends Stack.Parameterized[MethodBuilder[Req, Rep]] { self =>
  import MethodBuilder._

  override def toString: String =
    s"MethodBuilder(dest=$dest, stack=$stack, params=$params, config=$config)"

  //
  // Stack.Configured machinery
  //

  def withParams(ps: Stack.Params): MethodBuilder[Req, Rep] =
    new MethodBuilder[Req, Rep](
      methodPool,
      dest,
      stack,
      params ++ ps,
      config
    )

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
   * import com.twitter.conversions.DurationOps._
   * import com.twitter.finagle.client.MethodBuilder
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withTimeout.total(200.milliseconds)
   * }}}
   *
   * @example A per-request timeout of 50 milliseconds:
   * {{{
   * import com.twitter.conversions.DurationOps._
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
   * @param minSendBackupAfterMs Use a minimum non-zero delay to prevent sending unnecessary backup requests
   *                             immediately for services where the latency at the percentile where a
   *                             backup will be sent is ~0ms.
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retried.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
   *
   * @note See `idempotent` below for a version that takes a [[Tunable[Double]]] for `maxExtraLoad`.
   */
  def idempotent(
    maxExtraLoad: Double,
    sendInterrupts: Boolean,
    minSendBackupAfterMs: Int,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    val brfParam =
      if (maxExtraLoad == 0) BackupRequestFilter.Disabled
      else BackupRequestFilter.Configured(maxExtraLoad, sendInterrupts, minSendBackupAfterMs)
    addBackupRequestFilterParamAndClassifier(brfParam, classifier)
  }

  /**
   * Configure that requests are to be treated as idempotent. Because requests can be safely
   * retried, [[BackupRequestFilter]] is configured with the params maxExtraLoad and
   * sendInterrupts to decrease tail latency by sending an additional fraction of requests.
   *
   * @param maxExtraLoad How much extra load, as a fraction, we are willing to send to the server.
   *                     Must be between 0.0 and 1.0.
   *
   * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
   *                       is returned and the result of the outstanding request is superseded. For
   *                       protocols without a control plane, where the connection is cut on
   *                       interrupts, this should be "false" to avoid connection churn.
   *
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retried.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
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
   * @param minSendBackupAfterMs Use a minimum non-zero delay to prevent sending unnecessary backup requests
   *                             immediately for services where the latency at the percentile where a
   *                             backup will be sent is ~0ms.
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retried.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
   */
  def idempotent(
    maxExtraLoad: Tunable[Double],
    sendInterrupts: Boolean,
    minSendBackupAfterMs: Int,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    addBackupRequestFilterParamAndClassifier(
      BackupRequestFilter.Configured(maxExtraLoad, sendInterrupts, minSendBackupAfterMs),
      classifier
    )
  }

  /**
   * Configure that requests are to be treated as idempotent. Because requests can be safely
   * retried, [[BackupRequestFilter]] is configured with the params maxExtraLoad and
   * sendInterrupts to decrease tail latency by sending an additional fraction of requests.
   *
   * @param maxExtraLoad How much extra load, as a fraction, we are willing to send to the server.
   *                     Must be between 0.0 and 1.0.
   *
   * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
   *                       is returned and the result of the outstanding request is superseded. For
   *                       protocols without a control plane, where the connection is cut on
   *                       interrupts, this should be "false" to avoid connection churn.
   *
   * @param classifier [[ResponseClassifier]] (combined (via [[ResponseClassifier.orElse]])
   *                   with any existing classifier in the stack params), used for determining
   *                   whether or not requests have succeeded and should be retried.
   *                   These determinations are also reflected in stats, and used by
   *                   [[FailureAccrualFactory]].
   */
  def idempotent(
    maxExtraLoad: Tunable[Double],
    sendInterrupts: Boolean,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    addBackupRequestFilterParamAndClassifier(
      BackupRequestFilter.Configured(maxExtraLoad, sendInterrupts),
      classifier
    )
  }

  private[this] def idempotentify(
    applicationClassifier: Option[ResponseClassifier],
    protocolClassifier: ResponseClassifier
  ): ResponseClassifier = {
    val combined = applicationClassifier match {
      case Some(classifier) => classifier.orElse(protocolClassifier)
      case None => protocolClassifier
    }
    ResponseClassifier.named(s"Idempotent($combined)") {
      case reqrep if combined.isDefinedAt(reqrep) =>
        val result = combined(reqrep)
        if (result == ResponseClass.NonRetryableFailure) ResponseClass.RetryableFailure
        else result
    }
  }

  private[this] def addBackupRequestFilterParamAndClassifier(
    brfParam: BackupRequestFilter.Param,
    classifier: service.ResponseClassifier
  ): MethodBuilder[Req, Rep] = {
    val configClassifier = config.retry.underlyingClassifier
    val idempotentedConfigClassifier = idempotentify(configClassifier, classifier)

    val base = new MethodBuilder[Req, Rep](
      methodPool,
      dest,
      stack,
      // If the RetryBudget is not configured, BackupRequestFilter and RetryFilter will each
      // get a new instance of the default budget. Since we want them to share the same
      // client retry budget, insert the budget into the params.
      params + params[Retries.Budget],
      config.copy(backup = brfParam)
    )

    base.withRetry.forClassifier(idempotentedConfigClassifier)
  }

  private[this] def nonidempotentify(
    applicationClassifier: Option[ResponseClassifier]
  ): ResponseClassifier = {
    applicationClassifier match {
      case Some(classifier) =>
        ResponseClassifier.RetryOnWriteExceptions.orElse(
          ResponseClassifier.named(s"NonIdempotent($classifier)") {
            case reqrep if classifier.isDefinedAt(reqrep) =>
              val result = classifier(reqrep)
              if (result == ResponseClass.RetryableFailure) ResponseClass.NonRetryableFailure
              else result
          }
        )
      case None => ResponseClassifier.RetryOnWriteExceptions
    }
  }

  /**
   * Configure that requests are to be treated as non-idempotent. [[BackupRequestFilter]] is
   * disabled, and only those failures that are known to be safe to retry (i.e., write failures,
   * where the request was never sent) are retried via requeue filter; any previously configured
   * retries are removed.
   */
  def nonIdempotent: MethodBuilder[Req, Rep] = {
    val configClassifier =
      if (config.retry.responseClassifier == ResponseClassifier.Default) None
      else Some(config.retry.responseClassifier)
    val nonidempotentedConfigClassifier = nonidempotentify(configClassifier)

    new MethodBuilder[Req, Rep](
      methodPool,
      dest,
      stack,
      params,
      config.copy(backup = BackupRequestFilter.Param.Disabled)
    ).withRetry.forClassifier(nonidempotentedConfigClassifier)
  }

  /**
   * Allow customizations for protocol-specific trace initialization.
   */
  def withTraceInitializer(initializer: Filter.TypeAgnostic): MethodBuilder[Req, Rep] =
    withConfig(config.copy(traceInitializer = initializer))

  /**
   * Configure the customized filters, this is for generic configuration other
   * than TraceInitializer, Retry, and Timeout. Example usage is for applying
   * protocol-specific filters for MethodBuilder.
   *
   * Defaults is a pass-through [[TypeAgnostic]].
   *
   * @param filter A filter or filter chain to create a new MethodBuilder instance.
   */
  private[finagle] def filtered(filter: Filter.TypeAgnostic): MethodBuilder[Req, Rep] =
    withConfig(config.copy(filter = filter))

  //
  // Build
  //
  private[this] def newService(methodName: Option[String]): Service[Req, Rep] = {
    materialize()
    val withStats = withStatsForMethod(methodName)
    withStats.filters(methodName).andThen(withStats.wrappedService(methodName))
  }

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

  /**
   * Create a [[ServicePerEndpoint]] from the current configuration.
   */
  def newServicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    builder: ServicePerEndpointBuilder[Req, Rep, ServicePerEndpoint]
  ): ServicePerEndpoint = newServicePerEndpoint(builder, None)

  /**
   * Create a [[ServicePerEndpoint]] from the current configuration.
   */
  def newServicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    builder: ServicePerEndpointBuilder[Req, Rep, ServicePerEndpoint],
    methodName: String
  ): ServicePerEndpoint = newServicePerEndpoint(builder, Some(methodName))

  private[this] def newServicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    builder: ServicePerEndpointBuilder[Req, Rep, ServicePerEndpoint],
    methodName: Option[String]
  ): ServicePerEndpoint = {
    materialize()

    val withStats = withStatsForMethod(methodName)

    builder
      .servicePerEndpoint(withStats.wrappedService(methodName))
      .filtered(withStats.filters(methodName))
  }

  //
  // Internals
  //

  /**
   * '''For implementers'''
   *
   * Create a new instance of this [[MethodBuilder]] with the
   * `Config` modified.
   */
  private[client] def withConfig(config: Config): MethodBuilder[Req, Rep] =
    new MethodBuilder(methodPool, dest, stack, params, config)

  private[this] def clientName: String =
    params[param.Label].label match {
      case param.Label.Default => Showable.show(dest)
      case label => label
    }

  private[finagle] def statsReceiver(methodName: Option[String]): StatsReceiver = {
    val clientScoped = params[param.Stats].statsReceiver.scope(clientName)
    methodName match {
      case Some(name) => new LazyStatsReceiver(clientScoped.scope(name))
      case None => clientScoped
    }
  }

  private[this] def withStatsForMethod(methodName: Option[String]): MethodBuilder[Req, Rep] = {
    val sr = statsReceiver(methodName)
    // We update the backup request filter config (brfConfig) here with the method name
    // and stats receiver because this information wasn't available when `idemptotent` is called.
    configured(param.Stats(sr))
  }

  private[this] def materialize(): Unit = {
    methodPool.materialize(params)
  }

  private def filters(methodName: Option[String]): Filter.TypeAgnostic = {
    // Ordering of filters:
    // Requests start at the top and traverse down.
    // Responses flow back from the bottom up.
    //
    // Backups are positioned after retries to avoid request amplification of retries, after
    // total timeouts and before per-request timeouts so that each backup uses the per-request
    // timeout.
    //
    // - Trace Initialization
    // - Customized filter
    // - Logical Stats
    // - Failure logging
    // - Annotate method name for a `Failure`
    // - Total Timeout
    // - Retries
    // - Backup Requests
    // - Service (Finagle client's stack, including Per Request Timeout)

    val retries = withRetry
    val timeouts = withTimeout

    val failureSource = methodName match {
      case Some(name) => addFailureSource(name)
      case None => TypeAgnostic.Identity
    }

    config.traceInitializer
      .andThen(config.filter)
      .andThen(retries.logicalStatsFilter)
      .andThen(retries.logFailuresFilter(clientName, methodName))
      .andThen(failureSource)
      .andThen(timeouts.totalFilter)
      .andThen(retries.filter)
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

    ClientRegistry.register(
      entry,
      keyPrefix :+ "statsReceiver",
      params[param.Stats].statsReceiver.toString)

    withTimeout.registryEntries.foreach {
      case (suffix, value) =>
        ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
    withRetry.registryEntries.foreach {
      case (suffix, value) =>
        ClientRegistry.register(entry, keyPrefix ++ suffix, value)
    }
  }

  private class WrappedService(name: Option[String], underlying: Service[Req, Rep])
      extends ServiceProxy[Req, Rep](underlying)
      with CloseOnce {

    protected def dispatch(request: Req): Future[Rep] = underlying(request)
    protected def toClose: Closable = Closable.nop

    override def apply(request: Req): Future[Rep] =
      if (isClosed) Future.exception(new ServiceClosedException())
      else dispatch(request)

    override def status: Status =
      if (isClosed) Status.Closed
      else underlying.status

    override protected def closeOnce(deadline: Time): Future[Unit] = {
      // remove our method builder's entries from the registry
      ClientRegistry.unregisterPrefixes(registryEntry(), registryKeyPrefix(name))

      // call refCounted.close to decrease the ref count. `underlying.close` is only
      // called when the closable underlying `refCounted` is closed.
      Closable
        .sequence(
          methodPool,
          toClose,
          underlying
        ).close(deadline)
    }
  }

  private def wrappedService(name: Option[String]): Service[Req, Rep] = {
    addToRegistry(name)
    methodPool.open()

    val underlying = methodPool.get

    val backupRequestParams = params +
      config.backup +
      param.ResponseClassifier(config.retry.responseClassifier)

    // register BackupRequestFilter under the same prefixes as other method entries
    val prefixes = Seq(registryEntry().addr) ++ registryKeyPrefix(name)
    val backupsFilter = BackupRequestFilter
      .filterWithPrefixes[Any, Any](backupRequestParams, prefixes)

    // Depending on what underlying protocol we're dealing with, we either apply backups filter
    // right away or pass it down in a local to apply later (in ThriftMux, HTTP, "later" is after
    // partitioning).
    val supportsPerRequestBackups = stack.contains(DynamicBackupRequestFilter.role)

    backupsFilter match {
      case Some(filter) if supportsPerRequestBackups =>
        // Dynamic BRF is available. Let's set the local.
        new WrappedService(name, underlying) {
          override protected def dispatch(request: Req): Future[Rep] =
            DynamicBackupRequestFilter.let(filter) {
              underlying(request)
            }
          override protected def toClose: Closable = filter
        }

      case Some(filter) =>
        // Dynamic BRF is not available. Apply BRF right away.
        new WrappedService(name, underlying) {
          override protected def dispatch(request: Req): Future[Rep] =
            filter.asInstanceOf[Filter[Req, Rep, Req, Rep]].apply(request, underlying)
          override protected def toClose: Closable = filter
        }

      case None =>
        // BRF is disabled.
        new WrappedService(name, underlying)
    }
  }
}
