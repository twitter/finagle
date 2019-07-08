package com.twitter.finagle.client

import com.twitter.finagle.socks._
import com.twitter.finagle.{Address, Stack}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Duration, Future}
import java.net.{InetSocketAddress, SocketAddress}

/**
 * Transporters construct a `Future[Transport[In, Out, Context]]`.
 *
 * There is one Transporter assigned per remote peer.  Transporters are
 * symmetric to the server-side [[com.twitter.finagle.server.Listener]], except
 * that it isn't shared across remote peers..
 */
trait Transporter[In, Out, Ctx <: TransportContext] {
  def apply(): Future[
    Transport[In, Out] {
      type Context <: Ctx
    }
  ]

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
  import com.twitter.conversions.DurationOps._

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
  case class SocksProxy(
    sa: Option[SocketAddress],
    credentials: Option[(String, String)],
    bypassLocalhost: Boolean = true) {
    def mk(): (SocksProxy, Stack.Param[SocksProxy]) =
      (this, SocksProxy)
  }
  implicit object SocksProxy extends Stack.Param[SocksProxy] {

    private[this] def socksProxy: Option[SocketAddress] =
      (socksProxyHost.get, socksProxyPort.get) match {
        case (Some(host), Some(port)) => Some(new InetSocketAddress(host, port))
        case _ => None
      }

    private[this] def socksUsernameAndPassword: Option[(String, String)] =
      (socksUsernameFlag.get, socksPasswordFlag.get) match {
        case (Some(username), Some(password)) => Some((username, password))
        case _ => None
      }

    val default: SocksProxy =
      SocksProxy(socksProxy, socksUsernameAndPassword, !socksProxyForLocalhost())

    override def show(p: SocksProxy): Seq[(String, () => String)] = {
      // do not show the password for security reasons
      Seq(
        ("socketAddress", () => p.sa.toString),
        (
          "credentials",
          () =>
            p.credentials
              .map(c => s"username=${c._1}")
              .toString
        )
      )
    }
  }

  /**
   * $param a HttpProxy as the endpoint for a `Transporter`.
   * @see https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#9.9
   */
  case class HttpProxy(sa: Option[SocketAddress], credentials: Option[Credentials]) {
    def mk(): (HttpProxy, Stack.Param[HttpProxy]) =
      (this, HttpProxy)

    def this(sa: Option[SocketAddress]) = this(sa, None)
  }
  implicit object HttpProxy extends Stack.Param[HttpProxy] {
    val default: HttpProxy = HttpProxy(None, None)

    override def show(p: HttpProxy): Seq[(String, () => String)] = {
      // do not show the password for security reasons
      Seq(
        ("socketAddress", () => p.sa.toString),
        ("credentials", () => p.credentials.map(_.toStringNoPassword).toString)
      )
    }
  }

  case class HttpProxyTo(hostAndCredentials: Option[(String, Option[Credentials])])
  implicit object HttpProxyTo extends Stack.Param[HttpProxyTo] {
    val default: HttpProxyTo = HttpProxyTo(None)

    override def show(p: HttpProxyTo): Seq[(String, () => String)] = {
      // do not show the password for security reasons
      Seq(
        ("host", () => p.hostAndCredentials.map(_._1).toString),
        (
          "credentials",
          () =>
            p.hostAndCredentials
              .flatMap(_._2)
              .map(_.toStringNoPassword)
              .toString
        )
      )
    }
  }

  /**
   * This class wraps the username, password that we use for http proxy auth
   */
  case class Credentials(username: String, password: String) {
    private[Transporter] def toStringNoPassword: String =
      s"Credentials(username=$username)"
  }

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
