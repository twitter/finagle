package com.twitter.finagle.mux

import com.twitter.finagle.{Mux, Service, ServiceFactory, Stack, mux, param => fparam}
import com.twitter.finagle.Mux.Server.SessionF
import com.twitter.finagle.mux.pushsession._
import com.twitter.finagle.pushsession.RefPushSession
import com.twitter.io.{Buf, ByteReader}

// Implementation of the standard mux server that doesn't attempt to negotiate.
// Only useful for testing Smux to ensure that failing to negotiate doesn't circumvent TLS.
private object NonNegotiatingServer {

  private val NonNegotiatingSessionFactory: SessionF = (
    ref: RefPushSession[ByteReader, Buf],
    params: Stack.Params,
    sharedStats: SharedNegotiationStats,
    handle: MuxChannelHandle,
    service: Service[Request, Response]
  ) => {
    val statsReceiver = params.apply[fparam.Stats].statsReceiver

    val session = new MuxServerSession(
      params,
      new FragmentDecoder(sharedStats),
      new FragmentingMessageWriter(handle, Int.MaxValue, sharedStats),
      handle,
      service
    )

    ref.updateRef(session)
    ref
  }

  def apply(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Mux.server.stack,
    params: Stack.Params = Mux.server.params
  ): Mux.Server =
    Mux.Server(
      stack = stack,
      params = params,
      sessionFactory = NonNegotiatingSessionFactory
    )
}
