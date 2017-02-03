package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.service.{StatsFilter, TimeoutFilter}
import com.twitter.finagle.stats.{BlacklistStatsReceiver, ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.util.StackRegistry
import com.twitter.util.{Future, Promise, Time}
import java.util.concurrent.atomic.AtomicBoolean

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
    val params = stackClient.params
    val clientName = params[param.Label].label match {
      case param.Label.Default => dest
      case label => label
    }

    val needsTotalTimeoutModule =
      stackClient.stack.contains(TimeoutFilter.totalTimeoutRole)

    val service: Service[Req, Rep] = stackClient
      .withStack(modified(stackClient.stack))
      .withParams(stackClient.params)
      .newService(dest, param.Label.Default)
    // use reference counting so that the underlying Service can
    // be safely shared across methods and a close of one doesn't
    // effect the others.
    val refCounted = new RefcountedClosable(service)

    new MethodBuilder[Req, Rep](
      refCounted,
      clientName,
      dest,
      stackClient,
      Config.create(stackClient.params, needsTotalTimeoutModule))
  }

  private[this] def modified[Req, Rep](
    stack: Stack[ServiceFactory[Req, Rep]]
  ): Stack[ServiceFactory[Req, Rep]] = {
    stack
      // total timeouts are managed directly by MethodBuilder
      .remove(TimeoutFilter.totalTimeoutRole)
      // allow for dynamic per-request timeouts
      .replace(TimeoutFilter.role, DynamicTimeout.perRequestModule[Req, Rep])
  }

  private object Config {
    def create[Req, Rep](
      params: Stack.Params,
      stackHadTotalTimeout: Boolean
    ): Config[Req, Rep] = {
      Config(
        MethodBuilderRetry.newConfig(params),
        MethodBuilderTimeout.Config(stackHadTotalTimeout))
    }
  }

  private[client] case class Config[Req, Rep](
      retry: MethodBuilderRetry.Config[Req, Rep],
      timeout: MethodBuilderTimeout.Config)

  private val LogicalScope = "logical"

  /** Used by the `ClientRegistry` */
  private[client] val RegistryKey = "methods"

  // the `StatsReceiver` used is already scoped to `$clientName/$methodName/logical`.
  // this omits the pending gauge as well as failures/sourcedfailures details.
  private val LogicalStatsBlacklistFn: Seq[String] => Boolean = { segments =>
    val head = segments.head
    head == "pending" || head == "failures" || head == "sourcedfailures"
  }

}

/**
 * '''Experimental:''' This API is under construction.
 */
private[finagle] class MethodBuilder[Req, Rep] private (
    refCounted: RefcountedClosable[Service[Req, Rep]],
    clientName: String,
    dest: String,
    stackClient: StackClient[Req, Rep],
    private[client] val config: MethodBuilder.Config[Req, Rep]) { self =>
  import MethodBuilder._

  private[client] def params: Stack.Params =
    stackClient.params

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
   * Defaults to having no timeouts set.
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
    new MethodBuilderTimeout[Req, Rep](this)

  //
  // Build
  //

  /**
   * Create a [[Service]] from the current configuration.
   *
   * @param name used for scoping metrics
   */
  def newService(name: String): Service[Req, Rep] = {
    addToRegisty(name)
    val ret = filter(name).andThen(wrappedService(name))
    refCounted.open()
    ret
  }

  //
  // Internals
  //

  private[client] def withConfig(config: Config[Req, Rep]): MethodBuilder[Req, Rep] =
    new MethodBuilder(
      self.refCounted,
      self.clientName,
      self.dest,
      self.stackClient,
      config)

  private[this] def statsReceiver(name: String): StatsReceiver = {
    params[param.Stats].statsReceiver.scope(clientName, name)
  }

  private[this] def filter(name: String): Filter[Req, Rep, Req, Rep] = {
    // Ordering of filters:
    // Requests start at the top and traverse down.
    // Responses flow back from the bottom up.
    //
    // - Logical Stats
    // - Total Timeout
    // - Retries
    // - Service (Finagle client's stack, including Per Request Timeout)

    val stats = statsReceiver(name)

    statsFilter(name, stats)
      .andThen(withTimeout.totalFilter)
      .andThen(withRetry.filter(stats))
      .andThen(withTimeout.perRequestFilter)
  }

  private[this] def registryEntry(): StackRegistry.Entry =
    StackRegistry.Entry(dest, stackClient.stack, params)

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

  private[this] def wrappedService(name: String): Service[Req, Rep] =
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

  private[this] def statsFilter(name: String, stats: StatsReceiver): StatsFilter[Req, Rep] =
    new StatsFilter[Req, Rep](
      new BlacklistStatsReceiver(stats.scope(LogicalScope), LogicalStatsBlacklistFn),
      params[param.ResponseClassifier].responseClassifier,
      ExceptionStatsHandler.Null,
      params[StatsFilter.Param].unit)

}
