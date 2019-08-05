package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.param._
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import java.net.SocketAddress

private[finagle] object Http2Transporter {

  def apply(
    addr: SocketAddress,
    modifier: Transport[Any, Any] => Transport[Any, Any],
    params: Stack.Params
  ): Transporter[Any, Any, TransportContext] = {

    val tlsEnabled = params[Transport.ClientSsl].sslClientConfiguration.isDefined
    val isPriorKnowledge = params[PriorKnowledge].enabled

    if (isPriorKnowledge) {
      throw new IllegalStateException(s"Prior Knowledge not supported in ${getClass.getSimpleName}")
    } else if (tlsEnabled) {
      TlsTransporter.make(addr, modifier, params)
    } else {
      H2CTransporter.make(addr, modifier, params)
    }
  }
}
