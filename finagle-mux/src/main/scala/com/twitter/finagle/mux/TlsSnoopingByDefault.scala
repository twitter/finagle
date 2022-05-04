package com.twitter.finagle.mux

import com.twitter.finagle.server.ServerInfo

private[finagle] object TlsSnoopingByDefault {

  def ToggleName: String = "com.twitter.finagle.mux.TlsSnoopingByDefault"

  private[this] val toggle = Toggles(ToggleName)

  def apply(): Boolean = toggle(ServerInfo().id.hashCode)
}
