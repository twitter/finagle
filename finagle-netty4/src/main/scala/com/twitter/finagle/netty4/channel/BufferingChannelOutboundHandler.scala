package com.twitter.finagle.netty4.channel

import io.netty.channel._

/**
 * A trait that when mixed with [[ChannelOutboundHandler]] queues all outbound writes and
 * provides an API to fail/write/flush the underlying pending queue. By default it
 *
 * - fails all pending writes on `exceptionCaught` and propagates the exception down to the pipeline
 * - drains (and flushes if needed) all pending writes on `handlerRemoved`
 *
 * When mixed in, provides a protected API for
 *
 * - writing all pending writes and flushing if needed
 * - failing all pending writes with a given cause
 *
 * Most of the times, this is used (on the client side) to delay the connection promise satisfaction
 * (or channel active event) until we're sure that the connection is legit (eg: handshake is do) and
 * ready to accept traffic. While delaying the particular event, we also need to make sure no writes
 * are lost. This is why we buffer them until we're ready to either `failPendingWrites` or
 * `writePendingWritesAndFlushIfNeeded`. It usually makes sense to remove this handler from the
 * pipeline if it's no longer needed.
 */
private[finagle] trait BufferingChannelOutboundHandler extends ChannelOutboundHandler {

  private[this] var pendingWrites: PendingWriteQueue = _
  private[this] var needFlush: Boolean = false

  private def getWriteQueue(ctx: ChannelHandlerContext): PendingWriteQueue = {
    if (pendingWrites == null) {
      pendingWrites = new PendingWriteQueue(ctx)
    }

    pendingWrites
  }

  /**
   * Writes all the pending writes and also flushes the pipeline if it was requested before.
   */
  protected def writePendingWritesAndFlushIfNeeded(ctx: ChannelHandlerContext): Unit = {
    getWriteQueue(ctx).removeAndWriteAll()

    if (needFlush) {
      needFlush = false
      ctx.flush()
    }
  }

  /**
   * Fails all the pending writes with a given `cause`.
   */
  final protected def failPendingWrites(cause: Throwable): Unit = {
    if (pendingWrites != null) {
      pendingWrites.removeAndFailAll(cause)
    }
  }

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
    getWriteQueue(ctx).add(msg, promise)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    failPendingWrites(cause)
    ctx.fireExceptionCaught(cause)
  }

  override def flush(ctx: ChannelHandlerContext): Unit = {
    needFlush = true
  }

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    writePendingWritesAndFlushIfNeeded(ctx)
  }
}
