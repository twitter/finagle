package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.util.registry.GlobalRegistry
import io.netty.channel.epoll.Epoll

private[netty4] object nativeEpoll {
  private[this] val underlying: Toggle[Int] =
    Toggles("com.twitter.finagle.netty4.UseNativeEpoll")

  // evaluated once per VM for consistency between listeners, transporters + worker pool.
  lazy val enabled: Boolean = {
    val res = underlying(ServerInfo().id.hashCode) && Epoll.isAvailable
    GlobalRegistry.get.put(Seq("library", "netty4", "native epoll enabled"), res.toString)
    res
  }
}