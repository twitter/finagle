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
   * NOTE: Here is a brief summary of the configurable params.
   *
   * A configuration with a `threshold` of N% and a `window` of duration W
   * roughly translates as, "start dropping some requests to the cluster when
   * the nack rate averages at least N% over a window of duration W."
   *
   * Here are some examples of situations with param values chosen to make the
   * filter useful:
   *
   * - Owners of Service A examine their service's nack rate over several days
   *   and find that it is almost always under 10% and rarely above 1% (e.g.,
   *   during traffic spikes) or 5% (e.g., during a data center outage). They
   *   do not want to preemptively drop requests unless the cluster sees an
   *   extreme overload situation so they choose a nack rate threshold of 20%.
   *   And in such a situation they want the filter to act relatively quickly,
   *   so they choose a window of 30 seconds.
   *
   * - Owners of Service B observe that excess load typically causes peak nack
   *   rates of around 25% for up to 60 seconds. They want to be aggressive
   *   about avoiding cluster overload and don’t mind dropping some innocent
   *   requests during mild load so they choose a window of 10 seconds and a
   *   threshold of 0.15 (= 15%).
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
