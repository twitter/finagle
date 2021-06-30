package com.twitter.finagle.netty4

import com.twitter.concurrent.NamedPoolThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup

/**
 * Utilities to create and reuse Netty 4 boss threads.
 */
private object BossEventLoop {

  private def epoll(): EventLoopGroup =
    new EpollEventLoopGroup(
      1 /*nThreads*/,
      new NamedPoolThreadFactory("finagle/netty4/boss", makeDaemons = true)
    )

  private def nio(): EventLoopGroup =
    new NioEventLoopGroup(
      1 /*nThreads*/,
      new NamedPoolThreadFactory("finagle/netty4/boss", makeDaemons = true)
    )

  lazy val Global: EventLoopGroup =
    if (useNativeEpoll() && Epoll.isAvailable) epoll()
    else nio()
}
