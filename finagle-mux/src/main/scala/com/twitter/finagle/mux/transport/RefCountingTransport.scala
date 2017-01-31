package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.Bufs
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.io.Buf
import io.netty.channel.Channel

/**
 * a ChannelTransport which manages Bufs backed by direct buffers.
 */
private[finagle] class RefCountingTransport(ch: Channel)
  extends ChannelTransport(
    ch,
    releaseMessage = {
      case b: Buf => Bufs.releaseDirect(b)
      case _ => ()
    },
    replacePending = {
      case b: Buf => Bufs.copyAndReleaseDirect(b)
      case _ => ()
    }
  )
