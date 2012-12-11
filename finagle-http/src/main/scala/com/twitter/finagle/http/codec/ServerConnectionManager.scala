package com.twitter.finagle.http.codec

import org.jboss.netty.channel._
import com.twitter.finagle.netty3.Conversions._

/**
 * Deal with Http connection lifecycle wrt. keepalives, etc.
 *
 * See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html
 */

// > TODO
//   - should we support http/1.0 keepalives?
//   - should there be a general "connection dirtying" mechanism?
//   - keepalive policies (eg. max idle time)

// TODO: "If either the client or the server sends the close token in
// the Connection header, that request becomes the last one for the
// connection."

class ServerConnectionManager extends SimpleChannelHandler {
  private[this] val manager = new ConnectionManager

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    manager.observeMessage(e.getMessage)
    super.messageReceived(ctx, e)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    manager.observeMessage(e.getMessage)

    if (manager.shouldClose)
      e.getFuture.close()

    super.writeRequested(ctx, e)
  }
}
