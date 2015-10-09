package com.twitter.finagle.httpproxy

import com.twitter.finagle.client.Transporter.Credentials
import com.twitter.finagle.{ChannelClosedException, ConnectionFailedException, InconsistentStateException}
import com.twitter.io.Charsets
import com.twitter.util.Base64StringEncoder

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

/**
 * Handle SSL connections through a proxy that accepts HTTP CONNECT.
 *
 * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#9.9
 *
 */
object HttpConnectHandler {
  def addHandler(
    proxyAddr: SocketAddress,
    addr: InetSocketAddress,
    pipeline: ChannelPipeline,
    proxyCredentials: Option[Credentials]
  ): HttpConnectHandler = {
    val clientCodec = new HttpClientCodec()
    val handler = new HttpConnectHandler(proxyAddr, addr, clientCodec, proxyCredentials)
    pipeline.addFirst("httpProxyCodec", handler)
    pipeline.addFirst("clientCodec", clientCodec)
    handler
  }

  def addHandler(
    proxyAddr: SocketAddress,
    addr: InetSocketAddress,
    pipeline: ChannelPipeline
  ): HttpConnectHandler = {
    addHandler(proxyAddr, addr, pipeline, None)
  }
}

class HttpConnectHandler(
    proxyAddr: SocketAddress,
    addr: InetSocketAddress,
    clientCodec: HttpClientCodec,
    proxyCredentials: Option[Credentials])
  extends SimpleChannelHandler {
  import HttpConnectHandler._

  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)

  private[this] def fail(c: Channel, t: Throwable) {
    Option(connectFuture.get) foreach { _.setFailure(t) }
    Channels.close(c)
  }

  private[this] def writeRequest(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val hostNameWithPort = addr.getAddress.getHostName + ":" + addr.getPort
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, hostNameWithPort)
    req.headers().set("Host", hostNameWithPort)
    proxyCredentials.foreach { creds =>
      req.headers().set(HttpHeaders.Names.PROXY_AUTHORIZATION, proxyAuthorizationHeader(creds))
    }
    Channels.write(ctx, Channels.future(ctx.getChannel), req, null)
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    e match {
      case de: DownstreamChannelStateEvent =>
        if (!connectFuture.compareAndSet(null, e.getFuture)) {
          fail(ctx.getChannel, new InconsistentStateException(addr))
          return
        }

        // proxy cancellation
        val wrappedConnectFuture = Channels.future(de.getChannel, true)
        de.getFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isCancelled)
              wrappedConnectFuture.cancel()
          }
        })
        // Proxy failures here so that if the connect fails, it is
        // propagated to the listener, not just on the channel.
        wrappedConnectFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isSuccess || f.isCancelled)
              return

            fail(f.getChannel, f.getCause)
          }
        })

        val wrappedEvent = new DownstreamChannelStateEvent(
          de.getChannel, wrappedConnectFuture,
          de.getState, proxyAddr)

        super.connectRequested(ctx, wrappedEvent)

      case _ =>
        fail(ctx.getChannel, new InconsistentStateException(addr))
    }
  }

  // we delay propagating connection upstream until we've completed the proxy connection.
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(addr))
      return
    }

    // proxy cancellations again.
    connectFuture.get.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture) {
        if (f.isSuccess)
          HttpConnectHandler.super.channelConnected(ctx, e)

        else if (f.isCancelled)
          fail(ctx.getChannel, new ChannelClosedException(addr))
      }
    })

    writeRequest(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(addr))
      return
    }
    val resp = e.getMessage.asInstanceOf[DefaultHttpResponse]
    if (resp.getStatus == HttpResponseStatus.OK) {
      ctx.getPipeline.remove(clientCodec)
      ctx.getPipeline.remove(this)
      connectFuture.get.setSuccess()
    } else {
      val cause = new Throwable("unexpected response status received by HttpConnectHandler:"
        + resp.getStatus)

      fail(e.getChannel, new ConnectionFailedException(cause, addr))
    }
  }

  private[this] def proxyAuthorizationHeader(creds: Credentials) = {
    val bytes = "%s:%s".format(creds.username, creds.password).getBytes(Charsets.Utf8)
    "Basic " + Base64StringEncoder.encode(bytes)
  }
}
