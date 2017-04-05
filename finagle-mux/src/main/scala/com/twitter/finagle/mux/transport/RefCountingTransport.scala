package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.transport.ChannelTransport
import io.netty.channel.Channel

/**
 * a ChannelTransport which manages Bufs backed by direct buffers.
 */
private[finagle] class RefCountingTransport(ch: Channel) extends ChannelTransport(ch)
