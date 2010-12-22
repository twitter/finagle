package com.twitter.finagle.http

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.util.Conversions._

/**
 * Deal with HTTP connection lifecycle wrt. keepalives, etc.
 *
 * See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html
 */

// > TODO
//   - should we support http/1.0 keepalives?
//   - should there be a general "connection dirtying" mechanism?
//   - keepalive policies (eg. max idle time)

class HttpServerConnectionLifecycleManager extends SimpleChannelHandler {
  @volatile private[this] var isKeepAlive = false

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    // TODO: what happens in a upload situation here -- can we get
    // chunked content?
    e.getMessage match {
      case request: HttpRequest =>
        isKeepAlive = HttpHeaders.isKeepAlive(request)

      case _ =>
        isKeepAlive = false
    }

    super.messageReceived(ctx, e)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case response: HttpResponse =>
        if (!response.isChunked && (response.getHeader(HttpHeaders.Names.CONTENT_LENGTH) eq null))
          isKeepAlive = false

        // We respect server-side termination as well.
        if (!HttpHeaders.isKeepAlive(response))
          isKeepAlive = false

      case _ =>
        ()
    }

    if (!isKeepAlive) {
      val shouldTerminate =
        e.getMessage match {
          case response: HttpResponse if !response.isChunked => true
          case chunk: HttpChunk if chunk.isLast => true
          case _ => false
        }

      if (shouldTerminate)
        e.getFuture.close()
    }

    super.writeRequested(ctx, e)
  }
}
