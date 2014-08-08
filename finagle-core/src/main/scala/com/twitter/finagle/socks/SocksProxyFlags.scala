package com.twitter.finagle
package socks

import com.twitter.app.GlobalFlag
import java.net.{SocketAddress, InetSocketAddress}

private[finagle] object socksProxyHost extends GlobalFlag("", "SOCKS proxy host") { override val name = "socksProxyHost" }
private[finagle] object socksProxyPort extends GlobalFlag(0, "SOCKS proxy port") { override val name = "socksProxyPort" }
private[finagle] object socksUsernameFlag extends GlobalFlag("", "SOCKS username") { override val name = "socksUsername" }
private[finagle] object socksPasswordFlag extends GlobalFlag("", "SOCKS password") { override val name = "socksPassword" }

private[finagle] object SocksProxyFlags {
  def socksProxy: Option[SocketAddress] =
    (socksProxyHost.get, socksProxyPort.get) match {
      case (Some(host), Some(port)) => Some(new InetSocketAddress(host, port))
      case _ => None
    }

  def socksUsernameAndPassword: Option[(String, String)] =
    (socksUsernameFlag.get, socksPasswordFlag.get) match {
      case (Some(username), Some(password)) => Some((username,password))
      case _ => None
    }
}
