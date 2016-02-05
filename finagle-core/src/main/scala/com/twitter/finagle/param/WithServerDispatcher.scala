package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
  * Provides the `withDispatcherConfig` API entry point
  */
trait WithServerDispatcher[A <: Stack.Parameterized[A], Req, Rep] { self: Stack.Parameterized[A] =>

  /**
    * An entry point for an API configuring ServerDispatchers
  */
  val withServerDispatcher: ServerDispatcherParams[A, Req, Rep] =
    new ServerDispatcherParams(self)
}

