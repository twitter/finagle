package com.twitter.finagle.mux

import com.twitter.finagle.{param => fparam}
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.{Message, MuxContext}
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.finagle.{Mux, Service, ServiceFactory, Stack, mux}
import com.twitter.io.Buf
import com.twitter.util.Closable

// Implementation of the standard mux server that doesn't attempt to negotiate.
// Only useful for testing Smux to ensure that failing to negotiate doesn't circumvent TLS.
private class NonNegotiatingServer(
  stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.server.stack,
  params: Stack.Params = Mux.server.params
) extends Mux.Server(stack, params) {
  override protected def copy1(
    stack: Stack[ServiceFactory[Request, Response]],
    params: Stack.Params
  ): NonNegotiatingServer = new NonNegotiatingServer(stack, params)

  override protected def newDispatcher(
    transport: Transport[Buf, Buf] {
      type Context <: MuxContext
    },
    service: Service[Request, Response]
  ): Closable = {
    val statsReceiver = params.apply[fparam.Stats].statsReceiver.scope("mux")
    val fparam.Tracer(tracer) = params.apply[fparam.Tracer]
    val Lessor.Param(lessor) = params.apply[Lessor.Param]
    val fparam.ExceptionStatsHandler(excRecorder) = params.apply[fparam.ExceptionStatsHandler]

    val negotiatedTrans = transport.map(Message.encode, Message.decode)

    val statsTrans =
      new StatsTransport(negotiatedTrans, excRecorder, statsReceiver.scope("transport"))

    mux.ServerDispatcher.newRequestResponse(statsTrans, service, lessor, tracer, statsReceiver)
  }
}
