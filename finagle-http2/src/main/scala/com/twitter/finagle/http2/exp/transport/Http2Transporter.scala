package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.param._
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import java.net.SocketAddress

private[finagle] object Http2Transporter {

  def apply(params: Stack.Params)(addr: SocketAddress): Transporter[Any, Any, TransportContext] = {

    val config = params[Transport.ClientSsl].sslClientConfiguration
    val tlsEnabled = config.isDefined

    if (tlsEnabled) TlsTransporter.make(addr, params)
    else if (params[PriorKnowledge].enabled) PriorKnowledgeTransporter.make(addr, params)
    else H2CTransporter.make(addr, params)
  }
}
