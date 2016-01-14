package com.twitter.finagle.param

import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.{stats, tracing, Stack}
import com.twitter.util

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

  /**
   * Configures this server or client with given [[stats.StatsReceiver]]
   * (default: [[stats.DefaultStatsReceiver]]).
   */
  def withStatsReceiver(statsReceiver: stats.StatsReceiver): A =
    self.configured(Stats(statsReceiver))

  /**
   * Configures this server or client with given [[util.Monitor]]
   * (default: [[com.twitter.finagle.util.DefaultMonitor]]).
   */
  def withMonitor(monitor: util.Monitor): A =
    self.configured(Monitor(monitor))

  /**
   * Configures this server or client with given [[tracing.Tracer]]
   * (default: [[com.twitter.finagle.tracing.DefaultTracer]]).
   */
  def withTracer(tracer: tracing.Tracer): A =
    self.configured(Tracer(tracer))

  /**
   * Configures this server or client with given
   * [[stats.ExceptionStatsHandler exception stats handler]].
   */
  def withExceptionStatsHandler(exceptionStatsHandler: stats.ExceptionStatsHandler): A =
    self.configured(ExceptionStatsHandler(exceptionStatsHandler))

  /**
   * Configures the request `timeout` of this server or client (default: unbounded).
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
   * @see [[http://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def withRequestTimeout(timeout: util.Duration): A =
    self.configured(TimeoutFilter.Param(timeout))
}
