package com.twitter.finagle.socks

import com.twitter.app.GlobalFlag

private[finagle] object socksProxyHost extends GlobalFlag("", "SOCKS proxy host") {
  override val name = "socksProxyHost"
}
private[finagle] object socksProxyPort extends GlobalFlag(0, "SOCKS proxy port") {
  override val name = "socksProxyPort"
}
private[finagle] object socksUsernameFlag extends GlobalFlag("", "SOCKS username") {
  override val name = "socksUsername"
}
private[finagle] object socksPasswordFlag extends GlobalFlag("", "SOCKS password") {
  override val name = "socksPassword"
}
