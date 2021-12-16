package com.twitter.finagle.server

import com.twitter.finagle._
import com.twitter.finagle.exp.ForkingSchedulerFilter
import com.twitter.finagle.filter._
import com.twitter.finagle.param._
import com.twitter.finagle.service.DeadlineFilter
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.service.MetricBuilderRegistry
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.ServerStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.Stack
import com.twitter.finagle._
import com.twitter.jvm.Jvm

object StackServer {
  private[this] lazy val newJvmFilter = new MkJvmFilter(Jvm())

  private[this] class JvmTracing[Req, Rep]
      extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
    def role: Stack.Role = Role.jvmTracing
    def description: String = "Server-side JVM tracing"
    def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
      val tracer = _tracer.tracer
      if (tracer.isNull) next
      else newJvmFilter[Req, Rep].andThen(next)
    }
  }

  object ProtoTracing {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.protoTracing
        val description: String =
          "Pre-allocated stack module for protocols to inject tracing"
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
      }
  }

  object Preparer {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.preparer
        val description: String =
          "Prepares the server"
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
      }
  }

  /**
   * Canonical Roles for each Server-related Stack modules.
   */
  object Role extends Stack.Role("StackServer") {

    /**
     * Server-side JVM tracing
     */
    val jvmTracing: Stack.Role = Stack.Role("JvmTracing")

    /**
     * Prepares the server for transport-level connection
     */
    val preparer: Stack.Role = Stack.Role("preparer")

    /**
     * Defines a pre-allocated position in the stack for protocols to inject tracing.
     */
    val protoTracing: Stack.Role = Stack.Role("protoTracing")
  }

  /**
   * Creates a default finagle server [[com.twitter.finagle.Stack]].
   * The default stack can be configured via [[com.twitter.finagle.Stack.Param]]'s
   * in the finagle package object ([[com.twitter.finagle.param]]) and specific
   * params defined in the companion objects of the respective modules.
   *
   * @see [[com.twitter.finagle.filter.OffloadFilter]]
   * @see [[com.twitter.finagle.tracing.ServerDestTracingFilter]]
   * @see [[com.twitter.finagle.service.TimeoutFilter]]
   * @see [[com.twitter.finagle.service.DeadlineFilter]]
   * @see [[com.twitter.finagle.filter.DtabStatsFilter]]
   * @see [[com.twitter.finagle.service.StatsFilter]]
   * @see [[com.twitter.finagle.filter.RequestSemaphoreFilter]]
   * @see [[com.twitter.finagle.filter.ExceptionSourceFilter]]
   * @see [[com.twitter.finagle.filter.MkJvmFilter]]
   * @see [[com.twitter.finagle.tracing.ServerTracingFilter]]
   * @see [[com.twitter.finagle.tracing.TraceInitializerFilter]]
   * @see [[com.twitter.finagle.filter.MonitorFilter]]
   * @see [[com.twitter.finagle.filter.ServerStatsFilter]]
   * @see [[com.twitter.finagle.tracing.WireTracingFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    /*
     * NB on orientation: we here speak of "up" / "down" or "above" /
     * "below" in terms of a request's traversal of the stack---a
     * request starts at the top and goes down, a response returns
     * back up. This is opposite to how modules are written on the
     * page; a request starts at the bottom of the `newStack` method
     * and goes up.
     *
     * Also note that the term "stack" does not refer to a stack in the
     * computer science sense but instead in the sense of a chain of objects,
     * i.e., stack modules. Because modules are composed sequentially, it also
     * makes sense to speak of modules coming "before" or "after" others.
     *
     * Lastly, note that "module A comes before module B" has the same meaning
     * as "module A is pushed after module B".
     */

    val stk = new StackBuilder[ServiceFactory[Req, Rep]](stack.nilStack[Req, Rep])

    stk.push(ServerTracingFilter.module)

    // this goes near the bottom of the stack so it is close to where Service.apply happens.
    stk.push(ThreadUsage.module)

    // `ExportSslUsage` exports the TLS parameter to the R* Registry
    stk.push(ExportSslUsage.module)

    // We want to start expiring services as close to their instantiation
    // as possible. By installing `ExpiringService` here, we are guaranteed
    // to wrap the server's dispatcher.
    stk.push(ExpiringService.server)
    stk.push(ServerDestTracingFilter.module)
    stk.push(TimeoutFilter.serverModule)
    // The DeadlineFilter is pushed before the stats filters so stats are
    // recorded for the request. If a server processing deadlines is set in
    // TimeoutFilter, the deadline will start from the current time, and
    // therefore not be expired if the request were to then pass through
    // DeadlineFilter. Thus, DeadlineFilter is pushed after TimeoutFilter.
    stk.push(DeadlineFilter.module)
    stk.push(DtabStatsFilter.module)
    // Admission Control filters are inserted before `StatsFilter` so that rejected
    // requests are counted. We may need to adjust how latency are recorded
    // to exclude Nack response from latency stats, CSL-2306.
    stk.push(ServerAdmissionControl.module)
    stk.push(ConcurrentRequestFilter.module)
    stk.push(MaskCancelFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.push(new JvmTracing)
    stk.push(ServerStatsFilter.module)
    stk.push(ProtoTracing.module)
    // `WriteTracingFilter` annotates traced requests. Annotations are timestamped
    // so this should be low in the stack to accurately delineate between wire time
    // and handling time. Ideally this would live closer to the "wire" in the netty
    // pipeline but we do not have the appropriate hooks to do so with a properly
    // initialized context. Actually having these annotations still has value in
    // allowing us to provide a complimentary annotation to the Client WR/WS as well
    // as measure queueing within the server via ConcurrentRequestFilter.
    stk.push(WireTracingFilter.serverModule)

    // forks the execution if the current scheduler supports forking
    stk.push(ForkingSchedulerFilter.server)
    // This module is placed at the top of the stack and shifts Future execution context
    // from IO threads into a configured FuturePool right after Netty.
    stk.push(OffloadFilter.server)
    // The StatsFilter needs to be above the OffloadFilter so that we can
    // calculate latency metric changes when there's an offload delay.
    stk.push(StatsFilter.module)
    stk.push(Preparer.module)

    // The TraceInitializerFilter must be pushed after most other modules so that
    // any Tracing produced by those modules is enclosed in the appropriate
    // span.
    stk.push(TraceInitializerFilter.serverModule)
    stk.push(MonitorFilter.serverModule)

    stk.result
  }

  /**
   * The default params used for StackServers.
   * @note The MetricBuilderRegistry is stateful for each stack,
   *       this should be evaluated every time calling,
   */
  def defaultParams: Stack.Params =
    Stack.Params.empty + Stats(ServerStatsReceiver) +
      MetricBuilders(Some(new MetricBuilderRegistry()))

  /**
   * A set of StackTransformers for transforming server stacks.
   */
  private[finagle] object DefaultTransformer extends StackTransformerCollection
}

/**
 * A [[com.twitter.finagle.Server]] that composes a
 * [[com.twitter.finagle.Stack]].
 *
 * @see [[ListeningServer]] for a template implementation that tracks session resources.
 */
trait StackServer[Req, Rep]
    extends StackBasedServer[Req, Rep]
    with Stack.Parameterized[StackServer[Req, Rep]]
    with Stack.Transformable[StackServer[Req, Rep]] {

  /**
   * @see [[withStack]]
   */
  def transformed(t: Stack.Transformer): StackServer[Req, Rep] =
    withStack(t(stack))

  /** The current stack used in this StackServer. */
  def stack: Stack[ServiceFactory[Req, Rep]]

  /** The current parameter map used in this StackServer. */
  def params: Stack.Params

  /**
   *
   * A new [[StackServer]] with the provided [[Stack]].
   *
   * @see `withStack` that takes a `Function1` for a more ergonomic
   *     API when used with method chaining.
   */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackServer[Req, Rep]

  /**
   * A new [[StackServer]] using the function to create a new [[Stack]].
   *
   * The input to `fn` is the [[stack client's current stack]].
   * This API allows for easier usage when writing code that
   * uses method chaining.
   *
   * This method is similar to [[transformed]] while providing easier API
   * ergonomics for one-off `Stack` changes.
   *
   * @example
   * From Scala:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.server.withStack(_.prepend(MyStackModule))
   * }}}
   *
   * From Java:
   * {{{
   * import com.twitter.finagle.Http;
   * import static com.twitter.util.Function.func;
   *
   * Http.server().withStack(func(stack -> stack.prepend(MyStackModule)));
   * }}}
   *
   * @see [[withStack(Stack)]]
   * @see [[transformed]]
   */
  def withStack(
    fn: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]
  ): StackServer[Req, Rep] =
    withStack(fn(stack))

  def withParams(ps: Stack.Params): StackServer[Req, Rep]

  def configured[P: Stack.Param](p: P): StackServer[Req, Rep]

  def configured[P](psp: (P, Stack.Param[P])): StackServer[Req, Rep]

  def configuredParams(params: Stack.Params): StackServer[Req, Rep]
}
