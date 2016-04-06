package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.tracing.TraceId

/**
  * A collection of methods for configuring a [[ServerDispatcher]]
  * @tparam A a [[Stack.Parameterized]] server to configure
  */
class ServerDispatcherParams[A <: Stack.Parameterized[A]] (self: Stack.Parameterized[A]) {

  /**
    * Provides the hook to convert a Request to a
    *  [[com.twitter.finagle.tracing.TraceId TraceId]]
    * @param f a function `Any` to `Option[TraceId]`
    */
  def requestToTraceId(f: (Any) => Option[TraceId]): A =
    self.configured(self.params[ReqRepToTraceId].copy(fReq = f))

  /**
    * Provides the hook to convert a Response to a
    *  [[com.twitter.finagle.tracing.TraceId TraceId]]
    * @param f a function `Any` to `Option[TraceId]`
    */
  def responseToTraceId(f: (Any) => Option[TraceId]): A =
    self.configured(self.params[ReqRepToTraceId].copy(fRep = f))

}
