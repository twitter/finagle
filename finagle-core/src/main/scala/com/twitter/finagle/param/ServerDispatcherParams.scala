package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.tracing.{Trace, TraceId}
import com.twitter.finagle.dispatch.ServerDispatcherConfig

/**
  * A collection of methods for configuring the [[Dispatcher]] for Finagle Servers.
  * @tparam A [[Stack.Parameterized]] server to configure
 */
final class ServerDispatcherParams[A <: Stack.Parameterized[A], Req, Rep]
  (self: Stack.Parameterized[A]) {

  implicit val param = Stack.Param(
    new ReqRepToTraceId[Req, Rep]((r: Req) => Trace.nextId, (r: Rep) => Trace.nextId)
  )

  def reqRepToTraceId(fs: ReqRepToTraceId[Req, Rep]): A =
    self.configured(fs)
}

