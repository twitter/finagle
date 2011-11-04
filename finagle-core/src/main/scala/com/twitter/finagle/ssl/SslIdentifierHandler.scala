package com.twitter.finagle.ssl

import java.lang.reflect.Method
import java.util.logging.Logger

import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelStateEvent, SimpleChannelUpstreamHandler
}

/**
 * Sets additional connection-level information (e.g. IP) in the identifier field of an
 * SSL engine after the channel has been connected
 */
class SslIdentifierHandler(o: Object, certificate: String, cipherSpec: String)
  extends SimpleChannelUpstreamHandler
{
  private[this] val log = Logger.getLogger(getClass().getName())
  private[this] val setIdentifierMethod: Option[Method] =
    try {
      Some(o.getClass().getMethod("setIdentifier", classOf[String]))
    } catch {
      case _: NoSuchMethodException => None
    }

  private[this] def setIdentifier(ctx: ChannelHandlerContext) {
    setIdentifierMethod foreach { method: Method =>
      val identifier = "cert=%s;cipher_spec=%s;remote=%s".format(
        certificate,
        cipherSpec,
        ctx.getChannel().getRemoteAddress()
      )

      method.invoke(o, identifier.asInstanceOf[AnyRef])
    }
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    setIdentifier(ctx)

    super.channelConnected(ctx, e)
  }
}
