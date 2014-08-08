package com.twitter.finagle.client

import com.twitter.finagle.socks.SocksProxyFlags
import com.twitter.finagle.Stack
import com.twitter.finagle.transport.Transport
import com.twitter.util.Duration
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Transporters are simple functions from a `SocketAddress` to a
 * `Future[Transport[In, Out]]`. They represent a transport layer session from a
 * client to a server. Transporters are symmetric to the server-side
 * [[com.twitter.finagle.server.Listener]].
 */
trait Transporter[In, Out] {
  def apply(addr: SocketAddress): Future[Transport[In, Out]]
}

/**
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
private[finagle] object Transporter {
  import com.twitter.conversions.time._

  /**
   * $param a `SocketAddress` that a `Transporter` connects to.
   */
  case class EndpointAddr(addr: SocketAddress)
  implicit object EndpointAddr extends Stack.Param[EndpointAddr] {
    private[this] val noAddr = new SocketAddress {
      override def toString = "noaddr"
    }
    val default = EndpointAddr(noAddr)
  }

  /**
   * $param the connect timeout of a `Transporter`.
   *
   * @param howlong A maximum amount of time a transport
   * is allowed to spend connecting. Only relevant if the transport
   * represents a client transport.
   */
  case class ConnectTimeout(howlong: Duration)
  implicit object ConnectTimeout extends Stack.Param[ConnectTimeout] {
    val default = ConnectTimeout(1.second)
  }

  /**
   * $param hostname verification, if TLS is enabled.
   * @see [[com.twitter.finagle.transport.Transport#TLSEngine]]
   */
  case class TLSHostname(hostname: Option[String])
  implicit object TLSHostname extends Stack.Param[TLSHostname] {
    val default = TLSHostname(None)
  }

  /**
   * $param a SocksProxy as the endpoint for a `Transporter`.
   */
  case class SocksProxy(sa: Option[SocketAddress], credentials: Option[(String, String)])
  implicit object SocksProxy extends Stack.Param[SocksProxy] {
    val default = SocksProxy(
      SocksProxyFlags.socksProxy,
      SocksProxyFlags.socksUsernameAndPassword
    )
  }

  /**
   * $param a HttpProxy as the endpoint for a `Transporter`.
   * @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#9.9
   */
  case class HttpProxy(sa: Option[SocketAddress])
  implicit object HttpProxy extends Stack.Param[HttpProxy] {
    val default = HttpProxy(None)
  }
}
