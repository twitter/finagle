package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.ssl.ApplicationProtocols
import com.twitter.finagle.Stack

/**
 * A class eligible for configuring which protocols to specify in the ALPN
 * handshake.
 */
private[finagle] case class Alpn(protocols: ApplicationProtocols)

private[finagle] object Alpn {
  implicit val param = Stack.Param(Alpn(ApplicationProtocols.Unspecified))
}
