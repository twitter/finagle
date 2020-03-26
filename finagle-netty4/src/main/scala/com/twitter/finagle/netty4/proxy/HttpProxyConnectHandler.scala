package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.{ChannelClosedException, ProxyConnectException}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.client.Transporter.Credentials
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.channel.ConnectPromiseDelayListeners._
import com.twitter.util.Base64StringEncoder
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}
import java.nio.charset.StandardCharsets.UTF_8
import java.net.SocketAddress

/**
 * An internal handler that upgrades the pipeline to delay connect-promise satisfaction until the
 * remote HTTP proxy server is ready to proxy traffic to an ultimate destination represented as
 * `host` (i.e., HTTP proxy connect procedure is successful).
 *
 * This enables "Tunneling TCP-based protocols (i.e., TLS/SSL) through Web proxy servers" [1] and
 * may be used with any TCP traffic, not only HTTP(S). See Squid documentation on this feature [2].
 *
 * @note We don't use Netty's implementation [3] here because it supports an opposite direction: the
 *       destination passed to `Channel.connect` is an ultimate target and the `HttpProxyHandler`
 *       is supposed to replace it with proxy addr (represented as a `SocketAddress`). This is the
 *       exact approach we used for Netty 3 implementation, but we don't do that anymore because we
 *       don't want to bypass Finagle's load balancers while resolving the proxy endpoint.
 *
 * @note This mixes in a [[BufferingChannelOutboundHandler]] so we can protect ourselves from
 *       channel handlers that write on `channelAdded` or `channelActive`.
 *
 * [1]: https://tools.ietf.org/html/draft-luotonen-web-proxy-tunneling-01
 * [2]: https://wiki.squid-cache.org/Features/HTTPS
 * [3]: https://github.com/netty/netty/blob/4.1/handler-proxy/src/main/java/io/netty/handler/proxy/HttpProxyHandler.java
 *
 * @param host the ultimate host a remote proxy server connects to
 *
 * @param credentialsOption optional credentials for a proxy server
 */
private[netty4] class HttpProxyConnectHandler(
  host: String,
  credentialsOption: Option[Transporter.Credentials],
  httpClientCodec: ChannelHandler = new HttpClientCodec()) // exposed for testing
    extends ChannelDuplexHandler
    with BufferingChannelOutboundHandler { self =>

  private[this] final def httpCodecKey = "httpProxyClientCodec"

  private[this] var connectPromise: ChannelPromise = _
  private[this] var proxyResponseStatus: HttpResponseStatus = _

  private[this] final def proxyAuthorizationHeader(c: Credentials): String = {
    val bytes = "%s:%s".format(c.username, c.password).getBytes(UTF_8)
    "Basic " + Base64StringEncoder.encode(bytes)
  }

  private[this] final def fail(t: Throwable): Unit = {
    // We "try" because it might be already cancelled and we don't need to handle
    // cancellations here - it's already done by `proxyCancellationsTo`.
    connectPromise.tryFailure(t)
    failPendingWrites(t)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    val proxyConnectPromise = ctx.newPromise()

    // Cancel new promise if an original one is canceled.
    // NOTE: We don't worry about cancelling/failing pending writes here since it will happen
    // automatically on channel closure.
    promise.addListener(proxyCancellationsTo(proxyConnectPromise, ctx))

    // Fail old promise if a new one is failed.
    proxyConnectPromise.addListener(new GenericFutureListener[NettyFuture[Any]] {
      override def operationComplete(f: NettyFuture[Any]): Unit =
        if (f.isSuccess) {
          // Add HTTP client codec so we can talk to an HTTP proxy.
          ctx.pipeline().addBefore(ctx.name(), httpCodecKey, httpClientCodec)

          // Create new connect HTTP proxy connect request.
          val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, host)
          req.headers().set(HttpHeaderNames.HOST, host)
          credentialsOption.foreach(c =>
            req.headers().add(HttpHeaderNames.PROXY_AUTHORIZATION, proxyAuthorizationHeader(c)))

          ctx.writeAndFlush(req)
          readIfNeeded(ctx)
        } else {
          // The connect request was cancelled or failed so the channel was never active. Since no
          // writes are expected from the previous handler, no need to fail the pending writes.
          if (!f.isCancelled) {
            promise.setFailure(f.cause())
          }
        }
    })

    // We propagate the pipeline with a new promise thereby delaying the original connect's
    // satisfaction.
    connectPromise = promise
    ctx.connect(remote, local, proxyConnectPromise)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    def finishOrRead(o: HttpObject): Unit = {
      ReferenceCountUtil.release(o)

      if (o.isInstanceOf[LastHttpContent]) finishProxyHandshake(ctx)
      else readIfNeeded(ctx)
    }

    msg match {
      case rep: HttpResponse =>
        // The first portion of a response has arrived so we capture the status.
        proxyResponseStatus = rep.status()
        finishOrRead(rep)

      case chunk: HttpContent =>
        // Some proxy servers send their error messages as HTML formatted documents so
        // we need to swallow them.
        finishOrRead(chunk)

      case other => ctx.fireChannelRead(other)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    fail(cause)
    ctx.fireExceptionCaught(cause) // we don't call super.exceptionCaught since we've already filed
    // both connect promise and pending writes in `fail`
    ctx.close() // close a channel since we've failed to perform an HTTP proxy handshake
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    fail(new ChannelClosedException(ctx.channel().remoteAddress()))
    ctx.fireChannelInactive()
  }

  private[this] final def finishProxyHandshake(ctx: ChannelHandlerContext): Unit = {
    // A remote HTTP proxy is ready to proxy traffic to an ultimate destination. We no longer
    // need HTTP proxy pieces in the pipeline.
    if (proxyResponseStatus == HttpResponseStatus.OK) {
      ctx.pipeline().remove(httpCodecKey)
      ctx.pipeline().remove(self) // drains pending writes when removed

      connectPromise.trySuccess()
    } else {
      val failure =
        new ProxyConnectException(
          s"Unexpected status returned from an HTTP proxy server: $proxyResponseStatus.",
          ctx.channel().remoteAddress()
        )

      fail(failure)
      ctx.close()
    }
  }

  private[this] final def readIfNeeded(ctx: ChannelHandlerContext): Unit = {
    // We issue a read request if auto-read is disabled.
    if (!ctx.channel().config().isAutoRead) {
      ctx.read()
    }
  }
}
