package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import io.netty.channel.epoll.Epoll

object nativeEpoll {
  val Id = "com.twitter.finagle.netty4.UseNativeEpollV2"

  private[this] val underlying: Toggle[Int] = Toggles(Id)

  // evaluated once per VM for consistency between listeners, transporters + worker pool.
  private[netty4] lazy val enabled: Boolean =
    underlying(ServerInfo().id.hashCode) && Epoll.isAvailable
}
