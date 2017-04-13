package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.{Bufs, ResourceAwareQueue}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.io.Buf
import io.netty.channel.Channel

private object RefCountingTransport {
  private[transport] val copyAndReleaseFn: PartialFunction[Any, Any] = {
    case b: Buf => Bufs.copyAndReleaseDirect(b)
  }
}

/**
 * a ChannelTransport which manages Bufs backed by direct buffers.
 */
private[finagle] class RefCountingTransport(
    ch: Channel,
    /* exposed for testing*/ private[transport] val maxPendingOffers: Int = Int.MaxValue)
  extends ChannelTransport(
    ch,
    new ResourceAwareQueue[Any](
      maxPendingOffers = maxPendingOffers,
      releaseFn = RefCountingTransport.copyAndReleaseFn
    )
  )
