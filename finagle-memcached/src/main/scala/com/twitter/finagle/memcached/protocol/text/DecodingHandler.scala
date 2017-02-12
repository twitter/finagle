package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.Failure
import com.twitter.io.Buf
import org.jboss.netty.channel._


/**
 * Decodes Bufs into Decodings.
 */
private[finagle] class DecodingHandler(private[this] val decoder: Decoder) extends SimpleChannelUpstreamHandler {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case buf: Buf =>
        val decoding: Decoding = decoder.decode(buf)
        if (decoding != null) {
          Channels.fireMessageReceived(ctx, decoding)
        }

      case typ => Channels.fireExceptionCaught(ctx, Failure(
        "unexpected type when decoding to Decoding." +
          s"Expected Buf, got ${typ.getClass.getSimpleName}."))
    }
}