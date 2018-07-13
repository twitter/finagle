package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.service.PendingRequestFilter
import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.util.Duration

/**
 * A collection of methods for configuring the admission control modules of Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 */
class ClientAdmissionControlParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures a limit on the maximum number of outstanding requests per
   * connection. Default is no limit.
   */
  def maxPendingRequests(requestLimit: Int): A = {
    val lim =
      if (requestLimit == Int.MaxValue) None
      else Some(requestLimit)

    self.configured(PendingRequestFilter.Param(limit = lim))
  }

  /**
   * Disables the `NackAdmissionFilter` if backing off during overload situations
   * is not desirable behavior. The `NackAdmissionFilter` is enabled by default.
   */
  def noNackAdmissionControl: A = {
    self.configured(NackAdmissionFilter.Disabled)
  }

  /**
   * Configures the `NackAdmissionFilter`. The `NackAdmissionFilter` is enabled
   * by default and configured with the default values which can be found in
   * [[com.twitter.finagle.filter.NackAdmssionFilter]].
   *
   * @param window Duration over which to average the ratio of nackd/non-nacked
   * responses.
   *
   * @param threshold The upper limit of the fraction of responses which are
   * nacks before the `NackAdmissionFilter` begins to drop requests.
   */
  def nackAdmissionControl(window: Duration, threshold: Double): A = {
    self.configured[NackAdmissionFilter.Param](
      NackAdmissionFilter.Param.Configured(window, threshold)
    )
  }
}
