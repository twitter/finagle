package com.twitter.finagle.httpproxy

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, DefaultHttpResponse, HttpClientCodec, HttpMethod, HttpResponseStatus, HttpVersion}
import org.jboss.netty.util.CharsetUtil

import com.twitter.finagle.{ChannelClosedException, ConnectionFailedException, InconsistentStateException}

/**
 * Handle SSL connections through a proxy that accepts HTTP CONNECT.
 *
 * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#9.9
 *
 */
object HttpConnectHandler {
  def addHandler(proxyAddr: SocketAddress, addr: InetSocketAddress, pipeline: ChannelPipeline): HttpConnectHandler = {
    val clientCodec = new HttpClientCodec()
    val handler = new HttpConnectHandler(proxyAddr, addr, clientCodec)
    pipeline.addFirst("httpProxyCodec", handler)
    pipeline.addFirst("clientCodec", clientCodec)
    handler
  }
}

class HttpConnectHandler(proxyAddr: SocketAddress, addr: InetSocketAddress, clientCodec: HttpClientCodec)
  extends SimpleChannelHandler
{
  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)

  private[this] def fail(c: Channel, t: Throwable) {
    Option(connectFuture.get) foreach { _.setFailure(t) }
    Channels.close(c)
  }

  private[this] def writeRequest(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, addr.getAddress.getHostName + ":" + addr.getPort)
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
      fail(e.getChannel, new ConnectionFailedException(null, addr))
    }
  }
}
