package com.twitter.finagle.param

import com.twitter.finagle.filter.OffloadFilter
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.service
import com.twitter.finagle.stats
import com.twitter.finagle.tracing
import com.twitter.util
import com.twitter.util.Duration
import com.twitter.util.FuturePool
import com.twitter.util.tunable.Tunable
import java.util.concurrent.ExecutorService

/**
 * A collection of methods for configuring common parameters (labels, stats receivers, etc)
 * shared between Finagle clients and servers.
 *
 * @tparam A a [[Stack.Parameterized]] server/client to configure
 */
trait CommonParams[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Configures this server or client with given `label` (default: empty string).
   *
   * The `label` value is used for stats reporting to scope stats reported
   * from different clients/servers to a single stats receiver.
   */
  def withLabel(label: String): A =
    self.configured(Label(label))

  /*
   * Configures this server or client with a set of `tags` (default: empty set).
   *
   * Tags associate Finagle clients and servers with a set of keywords. Labels
   * are simply Tags with a single keyword.
   *
   * Tags provide a general purpose configuration mechanism for functionality
   * that is not yet known. This is powerful, but also easily misused. As such,
   * be conservative in using them.
   *
   * Frameworks that create services for each endpoint should tag them with
   * endpoint metadata, e.g.,
   *
   * {{{
   * val showPost = Http.server.withLabels("GET", "/posts/show")
   * val createPost = Http.server.withLabels("POST", "PUT", "/posts/create")
   * }}}
   *
   * Note: Tags can't be used in place of Label (at least not quite yet). Label
   * will appear in metrics, but Tags do not.
   */
  def withLabels(keywords: String*): A =
    self.configured(Tags(keywords: _*))

  /**
   * Configures this server or client with given [[stats.StatsReceiver]]
   * (default: [[stats.DefaultStatsReceiver]]).
   */
  def withStatsReceiver(statsReceiver: stats.StatsReceiver): A =
    self.configured(Stats(statsReceiver))

  /**
   * Configures this server or client with given [[util.Monitor]]
   * (default: [[com.twitter.finagle.util.NullMonitor]]).
   *
   * Monitors are Finagle's out-of-band exception reporters. Whenever an exception is thrown
   * on a request path, it's reported to the monitor. The configured `Monitor` is composed
   * (see below for how composition works) with the default monitor implementation,
   * [[com.twitter.finagle.util.DefaultMonitor]], which logs these exceptions.
   *
   * Monitors are wired into the server or client stacks via
   * [[com.twitter.finagle.filter.MonitorFilter]] and are applied to the following kinds
   * of exceptions:
   *
   *  - Synchronous exceptions thrown on request path, `Service.apply(request)`
   *  - Asynchronous exceptions (failed futures) thrown on request path, `Service.apply(request)`
   *  - Exceptions thrown from `respond`, `onSuccess`, `onFailure` future callbacks
   *  - Fatal exceptions thrown from `map`, `flatMap`, `transform` future continuations
   *
   * Put it this way, we apply `Monitor.handle` to an exception if we would otherwise "lose" it,
   * i.e. when it's not connected to the `Future`, nor is it connected to the call stack.
   *
   * You can compose multiple monitors if you want to extend or override the standard behavior,
   * defined in `DefaultMonitor`.
   *
   * {{{
   *   import com.twitter.util.Monitor
   *
   *   val consoleMonitor = new Monitor {
   *     def handle(exc: Throwable): Boolean = {
   *       Console.err.println(exc.toString)
   *       false // continue handling with the next monitor (usually DefaultMonitor)
   *      }
   *   }
   *
   *   $.withMonitor(consoleMonitor)
   * }}}
   *
   * Returning `true` form within a monitor effectively terminates the monitor chain so no
   * exceptions are propagated down to the next monitor.
   */
  def withMonitor(monitor: util.Monitor): A =
    self.configured(Monitor(monitor))

  /**
   * Configures this server or client with given [[tracing.Tracer]]
   * (default: [[com.twitter.finagle.tracing.DefaultTracer]]).
   *
   * @note if you supply [[com.twitter.finagle.tracing.NullTracer]], no trace
   * information will be written, but this does not disable Finagle from
   * propagating trace information.  Instead, if traces are being aggregated
   * across your fleet, it will orphan subsequent spans.
   */
  def withTracer(tracer: tracing.Tracer): A =
    self.configured(Tracer(tracer))

  /**
   * Configure a [[com.twitter.finagle.service.ResponseClassifier]]
   * which is used to determine the result of a request/response.
   *
   * This allows developers to give Finagle the additional application-specific
   * knowledge necessary in order to properly classify responses. Without this,
   * Finagle cannot make judgements about application-level failures as it only
   * has a narrow understanding of failures (for example: transport level, timeouts,
   * and nacks).
   *
   * As an example take an HTTP server that returns a response with a 500 status
   * code. To Finagle this is a successful request/response. However, the application
   * developer may want to treat all 500 status codes as failures and can do so via
   * setting a [[com.twitter.finagle.service.ResponseClassifier]].
   *
   * ResponseClassifier is a [[PartialFunction]] and as such multiple classifiers can
   * be composed together via [[PartialFunction.orElse]].
   *
   * Response classification is independently configured on the client and server.
   * For client-side response classification using [[com.twitter.finagle.builder.ClientBuilder]],
   * see `com.twitter.finagle.builder.ClientBuilder.responseClassifier`
   *
   * @see `com.twitter.finagle.http.service.HttpResponseClassifier` for some
   * HTTP classification tools.
   *
   * @note If unspecified, the default classifier is
   * [[com.twitter.finagle.service.ResponseClassifier.Default]]
   * which is a total function fully covering the input domain.
   */
  def withResponseClassifier(responseClassifier: service.ResponseClassifier): A =
    self.configured(ResponseClassifier(responseClassifier))

  /**
   * Configures this server or client with given
   * [[stats.ExceptionStatsHandler exception stats handler]].
   */
  def withExceptionStatsHandler(exceptionStatsHandler: stats.ExceptionStatsHandler): A =
    self.configured(ExceptionStatsHandler(exceptionStatsHandler))

  /**
   * Configures the request `timeout` of this server or client (default: unbounded).
   *
   * If the request has not completed within the given `timeout`, the pending
   * work will be interrupted via [[com.twitter.util.Future.raise]].
   *
   * == Client's Request Timeout ==
   *
   * The client request timeout is the maximum amount of time given to a single request
   * (if there are retries, they each get a fresh request timeout). The timeout is applied
   * only after a connection has been acquired. That is: it is applied to the interval
   * between the dispatch of the request and the receipt of the response.
   *
   * == Server's Request Timeout ==
   *
   * The server request timeout is the maximum amount of time, a server is allowed to
   * spend handling the incoming request. Using the Finagle terminology, this is an amount
   * of time after which a non-satisfied future returned from the user-defined service
   * times out.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def withRequestTimeout(timeout: Duration): A =
    self.configured(TimeoutFilter.Param(timeout))

  /**
   * Configures the [[Tunable]] request `timeout` of this server or client (if applying the
   * [[Tunable]] produces a value of `None`, an unbounded timeout is used for the request).
   *
   * If the request has not completed within the [[Duration]] resulting from `timeout.apply()`,
   *  the pending work will be interrupted via [[com.twitter.util.Future.raise]].
   *
   * == Client's Request Timeout ==
   *
   * The client request timeout is the maximum amount of time given to a single request
   * (if there are retries, they each get a fresh request timeout). The timeout is applied
   * only after a connection has been acquired. That is: it is applied to the interval
   * between the dispatch of the request and the receipt of the response.
   *
   * == Server's Request Timeout ==
   *
   * The server request timeout is the maximum amount of time, a server is allowed to
   * spend handling the incoming request. Using the Finagle terminology, this is an amount
   * of time after which a non-satisfied future returned from the user-defined service
   * times out.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]] and
   *      [[https://twitter.github.io/finagle/guide/Configuration.html#tunables]]
   */
  def withRequestTimeout(timeout: Tunable[Duration]): A =
    self.configured(TimeoutFilter.Param(timeout))

  /**
   * Configures this server or client to shift user-defined computation ([[com.twitter.util.Future]]
   * callbacks and transformations) off of IO threads into a given [[FuturePool]].
   *
   * By default, Finagle executes all futures in the IO threads, minimizing context switches. Given
   * there is usually a fixed number of IO threads shared across a JVM process, it's critically
   * important to ensure they aren't being blocked by the application code, affecting system's
   * responsiveness. Shifting application-level work onto a dedicated [[FuturePool]] or
   * [[ExecutorService]] offloads IO threads, which may improve throughput in CPU-bound systems.
   *
   * As always, run your own tests before enabling this feature.
   */
  def withExecutionOffloaded(pool: FuturePool): A =
    self.configured(OffloadFilter.Param(pool))

  /**
   * Configures this server or client to shift user-defined computation ([[com.twitter.util.Future]]
   * callbacks and transformations) off of IO threads into a given [[ExecutorService]].
   *
   * By default, Finagle executes all futures in the IO threads, minimizing context switches. Given
   * there is usually a fixed number of IO threads shared across a JVM process, it's critically
   * important to ensure they aren't being blocked by the application code, affecting system's
   * responsiveness. Shifting application-level work onto a dedicated [[FuturePool]] or
   * [[ExecutorService]] offloads IO threads, which may improve throughput in CPU-bound systems.
   *
   * As always, run your own tests before enabling this feature.
   */
  def withExecutionOffloaded(executor: ExecutorService): A =
    self.configured(OffloadFilter.Param(executor))
}
