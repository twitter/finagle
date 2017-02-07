package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.logging.Logger
import io.netty.channel.epoll.Epoll

private[netty4] object nativeEpoll {
  private[this] val log = Logger.get
  private[this] val underlying: Toggle[Int] =
    Toggles("com.twitter.finagle.netty4.UseNativeEpoll")

  // evaluated once per VM for consistency between listeners, transporters + worker pool.
  lazy val enabled: Boolean = {
    val res = underlying(ServerInfo().id.hashCode) && Epoll.isAvailable
    log.info(s"native epoll enabled: $res")
    res
  }
}
