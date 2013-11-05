package com.twitter.finagle
package socks

import com.twitter.app.GlobalFlag
import java.net.{SocketAddress, InetSocketAddress}

private[finagle] object socksProxyHost extends GlobalFlag[String]("SOCKS proxy host") { override val name = "socksProxyHost" }
private[finagle] object socksProxyPort extends GlobalFlag[Int]("SOCKS proxy port") { override val name = "socksProxyPort" }

private[finagle] object SocksProxyFlags {
  def socksProxy: Option[SocketAddress] =
    (socksProxyHost.get, socksProxyPort.get) match {
      case (Some(host), Some(port)) => Some(new InetSocketAddress(host, port))
      case _ => None
    }
}
