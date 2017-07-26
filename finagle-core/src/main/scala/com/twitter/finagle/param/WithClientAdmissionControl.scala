package com.twitter.finagle.param

import com.twitter.finagle.Stack

trait WithClientAdmissionControl[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the clients' admission control
   */
  val withAdmissionControl: ClientAdmissionControlParams[A] =
    new ClientAdmissionControlParams(this)
}
