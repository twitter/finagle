package com.twitter.finagle.netty4.channel

import com.twitter.logging.Logger
import io.netty.channel._
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}

/**
 * This object provides [[GenericFutureListener]]s that are useful for implementing handlers
 * that delay a connect-promise until some additional step is done (i.e., HTTP proxy connect,
 * SSL handshake, etc).
 *
 * For example, this is used by:
 *
 * - [[com.twitter.finagle.netty4.ssl.client.SslClientVerificationHandler]]
 * - [[com.twitter.finagle.netty4.proxy.HttpProxyConnectHandler]]
 * - [[com.twitter.finagle.netty4.proxy.Netty4ProxyConnectHandler]]
 */
private[netty4] object ConnectPromiseDelayListeners {
  private val log = Logger.get(this.getClass)

  /**
   * Creates a new [[GenericFutureListener]] that cancels a given `promise` when the
   * [[NettyFuture]] it's listening on is cancelled.
   *
   * @note The future listener returned from this method also closes the channel if the `promise`
   *       is already satisfied.
   */
  def proxyCancellationsTo(
    promise: ChannelPromise,
    ctx: ChannelHandlerContext
  ): GenericFutureListener[NettyFuture[Any]] = new GenericFutureListener[NettyFuture[Any]] {
    override def operationComplete(f: NettyFuture[Any]): Unit =
      if (f.isCancelled) {
        if (!promise.cancel(true) && promise.isSuccess) {
          // New connect promise wasn't cancelled because it was already satisfied (connected) so
          // we need to close the channel to prevent resource leaks.
          // See https://github.com/twitter/finagle/issues/345
          ctx.close()
        }
      }
  }

  /**
   * Creates a new [[GenericFutureListener]] that fails a given `promise` when the
   * [[NettyFuture]] it's listening on is failed.
   */
  def proxyFailuresTo(promise: ChannelPromise): GenericFutureListener[NettyFuture[Any]] =
    new GenericFutureListener[NettyFuture[Any]] {
      override def operationComplete(f: NettyFuture[Any]): Unit =
        // We filter cancellation here since we assume it was proxied from the old promise and
        // is already being handled in `cancelPromiseWhenCancelled`.
        if (!f.isSuccess && !f.isCancelled) {
          // if we fail `tryFailure` and we didn't get a resource, it is up to whoever set the failure
          // to clean up resources.
          if (!promise.tryFailure(f.cause)) {
            if (!promise.isCancelled)
              log.warning(
                "Attempted to propagate failure forward for a Future that had already been set."
                  + " This may indicate that there are multiple \"owners\" for the promise who may mark it as complete."
                  + s" Promise: $promise")
          }
        }
    }
}
