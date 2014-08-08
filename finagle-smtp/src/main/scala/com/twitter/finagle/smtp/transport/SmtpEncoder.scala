package com.twitter.finagle.smtp.transport

import com.twitter.finagle.smtp.Request
import com.twitter.util.NonFatal
import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil

/**
 * Encodes a Request into a ChannelBuffer.
 */
class SmtpEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent): Unit =
    evt.getMessage match {
      case req: Request =>
        try {
          val buf = ChannelBuffers.copiedBuffer(req.cmd + "\r\n", CharsetUtil.US_ASCII)
          Channels.write(ctx, evt.getFuture, buf, evt.getRemoteAddress)
        } catch {
          case NonFatal(e) =>
            evt.getFuture.setFailure(new ChannelException(e.getMessage))
        }

      case unknown =>
        evt.getFuture.setFailure(new ChannelException(
          "Unsupported request type %s".format(unknown.getClass.getName)))
    }
}
