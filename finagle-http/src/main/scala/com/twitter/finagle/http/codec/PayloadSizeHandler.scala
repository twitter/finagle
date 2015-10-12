package com.twitter.finagle.http.codec

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

private[http] class PayloadSizeHandler(maxRequestPayloadSize: Int)
  extends SimpleChannelUpstreamHandler {

  require(maxRequestPayloadSize > -1, s"maxRequestPayloadSize must not be negative, was $maxRequestPayloadSize")

  override def messageReceived(ctx: ChannelHandlerContext, m: MessageEvent): Unit = m.getMessage match {
    case request: HttpRequest if HttpHeaders.getContentLength(request, -1) > maxRequestPayloadSize =>

      val tooLargeResponse =
        PayloadSizeHandler.mkTooLargeResponse(request.getProtocolVersion)
      val writeF = Channels.future(ctx.getChannel)

      // hang up after the 413 is sent.
      Channels.write(ctx, writeF, tooLargeResponse, m.getRemoteAddress)
      writeF.addListener(ChannelFutureListener.CLOSE)

    // todo: should we enforce the payload limit on responses?
    case _ => super.messageReceived(ctx, m)
  }
}

private[codec] object PayloadSizeHandler {
  def mkTooLargeResponse(version: HttpVersion): HttpResponse = {
    val resp = new DefaultHttpResponse(version, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
    HttpHeaders.setHeader(resp, HttpHeaders.Names.CONNECTION, "close")
    resp
  }
}
