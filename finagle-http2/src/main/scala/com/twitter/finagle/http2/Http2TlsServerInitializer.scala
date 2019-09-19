package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.server.ServerNpnOrAlpnHandler
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.channel.socket.SocketChannel

/**
 * This handler sets us up for protocol negotiation over TLS
 */
private[http2] class Http2TlsServerInitializer(
  init: ChannelInitializer[Channel],
  params: Stack.Params)
    extends ChannelInitializer[SocketChannel] {

  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    p.addLast(new ServerNpnOrAlpnHandler(init, params))
    p.addLast(init)
  }
}
