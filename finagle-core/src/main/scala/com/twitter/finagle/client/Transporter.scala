package com.twitter.finagle.client

import com.twitter.finagle.{Address, Stack}
import com.twitter.finagle.socks.SocksProxyFlags
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Future}
import java.net.SocketAddress

/**
 * Transporters construct a `Future[Transport[In, Out]]`.
 *
 * There is one Transporter assigned per remote peer.  Transporters are
 * symmetric to the server-side [[com.twitter.finagle.server.Listener]], except
 * that it isn't shared across remote peers..
 */
trait Transporter[In, Out] {
  def apply(): Future[Transport[In, Out]]

  /**
   * The address of the remote peer that this `Transporter` connects to.
   */
  def remoteAddress: SocketAddress
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
  case class EndpointAddr(addr: Address) {
    def mk(): (EndpointAddr, Stack.Param[EndpointAddr]) =
      (this, EndpointAddr.param)
  }
  object EndpointAddr {
    implicit val param =
      Stack.Param(EndpointAddr(Address.failing))
  }

  /**
   * $param the connect timeout of a `Transporter`.
   *
   * @param howlong Maximum amount of time a transport is allowed to
   *                spend connecting. Must be non-negative.
   */
  case class ConnectTimeout(howlong: Duration) {
    if (howlong < Duration.Zero)
      throw new IllegalArgumentException(s"howlong must be non-negative: saw $howlong")

    def mk(): (ConnectTimeout, Stack.Param[ConnectTimeout]) =
      (this, ConnectTimeout.param)
  }
  object ConnectTimeout {
    implicit val param = Stack.Param(ConnectTimeout(1.second))
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

  case class HttpProxyTo(hostAndCredentials: Option[(String, Option[Credentials])])
  object HttpProxyTo {
    implicit val param = Stack.Param(HttpProxyTo(None))
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
