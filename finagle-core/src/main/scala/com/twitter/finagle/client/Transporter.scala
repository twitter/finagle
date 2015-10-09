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
 * A collection of [[com.twitter.finagle.Stack.Param Stack.Params]] useful for configuring
 * a [[com.twitter.finagle.client.Transporter]].
 *
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
object Transporter {
  import com.twitter.conversions.time._

  /**
   * $param a `SocketAddress` that a `Transporter` connects to.
   */
  case class EndpointAddr(addr: SocketAddress) {
    def mk(): (EndpointAddr, Stack.Param[EndpointAddr]) =
      (this, EndpointAddr.param)
  }
  object EndpointAddr {
    implicit val param = Stack.Param(EndpointAddr(new SocketAddress {
      override def toString = "noaddr"
    }))
  }

  /**
   * $param the connect timeout of a `Transporter`.
   *
   * @param howlong A maximum amount of time a transport
   * is allowed to spend connecting.
   */
  case class ConnectTimeout(howlong: Duration) {
    def mk(): (ConnectTimeout, Stack.Param[ConnectTimeout]) =
      (this, ConnectTimeout.param)
  }
  object ConnectTimeout {
    implicit val param = Stack.Param(ConnectTimeout(1.second))
  }

  /**
   * $param hostname verification, if TLS is enabled.
   * @see [[com.twitter.finagle.transport.Transport#TLSEngine]]
   */
  case class TLSHostname(hostname: Option[String]) {
    def mk(): (TLSHostname, Stack.Param[TLSHostname]) =
      (this, TLSHostname.param)
  }
  object TLSHostname {
    implicit val param = Stack.Param(TLSHostname(None))
  }

  /**
   * $param a SocksProxy as the endpoint for a `Transporter`.
   */
  case class SocksProxy(sa: Option[SocketAddress], credentials: Option[(String, String)]) {
    def mk(): (SocksProxy, Stack.Param[SocksProxy]) =
      (this, SocksProxy.param)
  }
  object SocksProxy {
    implicit val param = Stack.Param(SocksProxy(
      SocksProxyFlags.socksProxy,
      SocksProxyFlags.socksUsernameAndPassword
    ))
  }

  /**
   * $param a HttpProxy as the endpoint for a `Transporter`.
   * @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#9.9
   */
  case class HttpProxy(sa: Option[SocketAddress], credentials: Option[Credentials]) {
    def mk(): (HttpProxy, Stack.Param[HttpProxy]) =
      (this, HttpProxy.param)

    def this(sa: Option[SocketAddress]) = this(sa, None)
  }
  object HttpProxy {
    implicit val param = Stack.Param(HttpProxy(None, None))
  }

  /**
   * This class wraps the username, password that we use for http proxy auth
   */
  case class Credentials(username: String, password: String)

  /**
   * Configures the traffic class to be used by clients.
   *
   * @param value `None` indicates no class specified. When `Some`, is an opaque
   * identifier and its meaning and interpretation are implementation specific.
   * Currently used to configure [[java.net.StandardSocketOptions.IP_TOS]].
   */
  case class TrafficClass(value: Option[Int]) {
    def mk(): (TrafficClass, Stack.Param[TrafficClass]) =
      (this, TrafficClass.param)
  }
  object TrafficClass {
    implicit val param = Stack.Param(TrafficClass(None))
  }

}
