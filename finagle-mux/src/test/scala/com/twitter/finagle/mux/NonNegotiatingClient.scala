package com.twitter.finagle.mux

import com.twitter.finagle.{param => fparam}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.{Mux, Service, ServiceFactory, Stack, mux}
import com.twitter.finagle.mux.transport.{Message, MuxContext}
import com.twitter.finagle.transport.{StatsTransport, Transport}
import com.twitter.io.Buf

// Implementation of the standard mux client that doesn't attempt to negotiate.
// Only useful for testing Smux to ensure that failing to negotiate doesn't circumvent TLS.
private class NonNegotiatingClient(
  stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.client.stack,
  params: Stack.Params = Mux.client.params
) extends Mux.Client(stack, params) {

  override protected def copy1(
    stack: Stack[ServiceFactory[Request, Response]],
    params: Stack.Params
  ): Mux.Client = new NonNegotiatingClient(stack, params)

  override protected def newDispatcher(
    transport: Transport[Buf, Buf] {
      type Context <: MuxContext
    }
  ): Service[Request, Response] = {
    val FailureDetector.Param(detectorConfig) = params.apply[FailureDetector.Param]
    val fparam.ExceptionStatsHandler(excRecorder) = params.apply[fparam.ExceptionStatsHandler]
    val fparam.Label(name) = params.apply[fparam.Label]
    val statsReceiver = params.apply[fparam.Stats].statsReceiver.scope("mux")

    val negotiatedTrans = transport.map(Message.encode, Message.decode)

    val statsTrans =
      new StatsTransport(negotiatedTrans, excRecorder, statsReceiver.scope("transport"))

    val session = new mux.ClientSession(statsTrans, detectorConfig, name, statsReceiver)

    mux.ClientDispatcher.newRequestResponse(session)
  }
}
