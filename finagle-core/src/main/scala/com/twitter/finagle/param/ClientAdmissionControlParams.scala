package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.service.PendingRequestFilter
import com.twitter.finagle.filter.NackAdmissionFilter

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
   * Disables the NackAdmissionFilter if backing off during overload situations
   * is not desirable behavior.
   */
  def noNackAdmissionControl: A = {
    self.configured(NackAdmissionFilter.Disabled)
  }
}
