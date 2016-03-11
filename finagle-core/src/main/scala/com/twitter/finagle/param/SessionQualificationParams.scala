package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.service.{FailureAccrualFactory, FailFastFactory}

/**
 * A collection of methods for configuring modules which help Finagle determine the health
 * of a session. Some of these act as
 * <a href=" https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern">circuit breakers</a>,
 * instructing the load balancer to choose alternate paths. Effectively these enable your
 * client to maintain higher success rates.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
 */
class SessionQualificationParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Disables the Fail Fast module on this client (default: enabled).
   *
   * The Fail Fast module is a Finagle per-session circuit breaker. It marks a host
   * unavailable upon connection failure. The host remains dead until we successfully connect.
   *
   * @note Usually, it's a good idea to disable Fail Fast for server sets with only
   *       one host in the load balancer's replica set.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#fail-fast]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailFastFactory]]
   */
  def noFailFast: A =
    self.configured(FailFastFactory.FailFast(enabled = false))

  /**
   * Disables the Failure Accrual module on this client (default: enabled).
   *
   * The Failure Accrual module is a Finagle per-request circuit breaker. It marks a
   * host unavailable depending on the used [[FailureAccrualPolicy policy]]. The default
   * setup for the Failure Accrual module is to use a policy based on the number of
   * consecutive failures (default is 5) accompanied by equal jittered backoff
   * producing durations for which a host is marked unavailable.
   *
   * @note Configuring Failure Accrual is experimental so `.configured` with
   *       `FailureAccrualFactory.Param` should be used to do that.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailureAccrualFactory]]
   */
  def noFailureAccrual: A =
    self.configured(FailureAccrualFactory.Disabled)
}
