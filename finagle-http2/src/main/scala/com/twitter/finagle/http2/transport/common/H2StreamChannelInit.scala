package com.twitter.finagle.http2.transport.common

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.NackRstFrameHandling
import com.twitter.finagle.http2.transport.client.Http2ClientEventMapper
import com.twitter.finagle.http2.transport.server.H2UriValidatorHandler
import com.twitter.finagle.netty4.http
import com.twitter.finagle.netty4.param.Allocator
import io.netty.channel._
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec

/**
 * Initializes the child channels for HTTP/2 StreamChannels to a state where they look like HTTP/1.x pipelines
 */
private[http2] object H2StreamChannelInit {

  def initClient(params: Stack.Params): ChannelInitializer[Channel] =
    new H2Initializer(None, params, isServer = false)

  def initServer(
    init: ChannelInitializer[Channel],
    params: Stack.Params
  ): ChannelInitializer[Channel] =
    new H2Initializer(Some(init), params, isServer = true)

  private final class H2Initializer(
    init: Option[ChannelInitializer[Channel]],
    params: Stack.Params,
    isServer: Boolean)
      extends ChannelInitializer[Channel] {
    private val initParams = if (isServer) http.initServer(params) else http.initClient(params)
    def initChannel(ch: Channel): Unit = {
      val alloc = params[Allocator].allocator
      ch.config.setAllocator(alloc)
      if (isServer) {
        if (params[NackRstFrameHandling].enabled) {
          ch.pipeline.addLast(new Http2NackHandler)
        }
        ch.pipeline.addLast(H2UriValidatorHandler.HandlerName, H2UriValidatorHandler)
      }

      ch.pipeline.addLast(
        new Http2StreamFrameToHttpObjectCodec(
          isServer,
          false /* validateHeaders */
        )
      )
      ch.pipeline.addLast(StripHeadersHandler.HandlerName, StripHeadersHandler)
      ch.pipeline.addLast(Http2StreamMessageHandler(isServer = isServer))

      initParams(ch.pipeline)

      if (!isServer) {
        ch.pipeline.addLast("event-mapper", Http2ClientEventMapper)
      }

      init match {
        case Some(init) => ch.pipeline.addLast(init)
        case None => () // nop
      }
    }
  }
}
