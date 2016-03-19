package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import io.netty.channel.{ChannelPipeline, ChannelInboundHandlerAdapter, ChannelHandlerContext}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2._
import io.netty.handler.logging.LogLevel

private[http2] object Http2Transporter {

  // constructing an http2 cleartext transport
  private[http2] def doToPipeline(pipeline: ChannelPipeline): Unit = {
    val connection = new DefaultHttp2Connection(false /*server*/)
    val adapter = new DelegatingDecompressorFrameListener(
      connection,
      new InboundHttp2ToHttpAdapterBuilder(connection)
        .maxContentLength(Int.MaxValue)
        .propagateSettings(true)
        .build()
    )
    val connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
      .frameListener(adapter)
      .frameLogger(new Http2FrameLogger(LogLevel.ERROR))
      .connection(connection)
      .build()

    val sourceCodec = new HttpClientCodec()
    val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
    val upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, Int.MaxValue)

    pipeline.addLast(sourceCodec,
      upgradeHandler,
      new UpgradeRequestHandler())
  }

  def apply(params: Stack.Params): Transporter[Any, Any] = {
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    Netty4Transporter[Any, Any](doToPipeline _, params + Netty4Transporter.Backpressure(false))
  }

  // borrows heavily from the netty http2 example
  class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {
    // TODO: should buffer writes on receiving settings from remote
    override def channelActive(ctx: ChannelHandlerContext): Unit = {

      // we send a regular http 1.1 request as an attempt to upgrade in the http2 cleartext protocol
      // it will be modified by HttpClientUpgradeHandler and Http2ClientUpgradeCodec to ensure we
      // upgrade to http2 properly.
      val upgradeRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      ctx.writeAndFlush(upgradeRequest)

      ctx.fireChannelActive()

      // Done with this handler, remove it from the pipeline.
      ctx.pipeline().remove(this)
    }
  }
}

