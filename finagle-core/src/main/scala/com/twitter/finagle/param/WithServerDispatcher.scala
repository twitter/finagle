package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
  * Provides the `withServerDispatcher` API entry point
  *
  * @see [[com.twitter.finagle.param.ServerDispatcherParams ServerDispatcherParams]]
  */
trait WithServerDispatcher[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
    * An entry point for configuring FOO BAR
    */
  val withServerDispatcher: ServerDispatcherParams[A] =
    new ServerDispatcherParams(self)
}
