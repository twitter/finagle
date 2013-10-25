package com.twitter.finagle
package socks

import com.twitter.app.GlobalFlag
import java.net.{SocketAddress, InetSocketAddress}

private[finagle] object SocksProxyFlags {
  object socksProxyHost extends GlobalFlag("", "SOCKS proxy host") { override val name = "socksProxyHost" }
  object socksProxyPort extends GlobalFlag(0, "SOCKS proxy port") { override val name = "socksProxyPort" }

  def socksProxy: Option[SocketAddress] = {
    (socksProxyHost(), socksProxyPort()) match {
      case ("", _) => None
      case (_, 0) => None
      case (host, port) => Some(new InetSocketAddress(host, port))
    }
  }
}
