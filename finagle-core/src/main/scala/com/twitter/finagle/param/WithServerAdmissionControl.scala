package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withAdmissionControl` API entry point.
 *
 * @see [[ServerAdmissionControlParams]]
 */
trait WithServerAdmissionControl[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the servers' admission control.
   */
  val withAdmissionControl: ServerAdmissionControlParams[A] =
    new ServerAdmissionControlParams(this)
}
