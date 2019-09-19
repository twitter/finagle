package com.twitter.finagle.http2.transport.client

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.netty4.transport.ChannelTransport
import io.netty.channel.Channel

/**
 * A `ChannelTransport` specialized for stream channels.
 *
 * It uses a vanilla `AsyncQueue` and omits stack traces on close for
 * performance reasons since each stream gets its own channel.
 */
private[finagle] class StreamChannelTransport(ch: Channel)
    extends ChannelTransport(
      ch = ch,
      readQueue = new AsyncQueue[Any],
      omitStackTraceOnInactive = true
    )
