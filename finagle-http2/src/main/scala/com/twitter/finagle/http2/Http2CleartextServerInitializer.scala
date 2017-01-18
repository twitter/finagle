package com.twitter.finagle.http2

import com.twitter.finagle.http
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.PriorKnowledgeHandler
import com.twitter.finagle.netty4.http.exp.{HttpCodecName, initServer}
import com.twitter.finagle.param.Stats
import com.twitter.logging.Logger
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer,
  ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{
  SourceCodec, UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{FullHttpRequest, HttpServerUpgradeHandler}
import io.netty.handler.codec.http2.{
  Http2Codec, Http2CodecUtil, Http2ServerDowngrader, Http2ServerUpgradeCodec, Http2ResetFrame}
import io.netty.util.AsciiString

/**
 * This handler sets us up for a cleartext upgrade
 */
private[http2] class Http2CleartextServerInitializer(
    init: ChannelInitializer[Channel],
    params: Stack.Params)
  extends ChannelInitializer[SocketChannel] {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  val initializer = new ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      ch.pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))

      // we want to drop reset frames because the Http2ServerDowngrader doesn't know what to
      // do with them, and our dispatchers expect to only get http/1.1 message types.
      ch.pipeline.addLast(new ChannelInboundHandlerAdapter() {
        override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
          if (!msg.isInstanceOf[Http2ResetFrame])
            super.channelRead(ctx, msg)
        }
      })
      initServer(params)(ch.pipeline)
      ch.pipeline.addLast(init)
    }
  }

  val upgradeCodecFactory: UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val initialSettings = Settings.fromParams(params)
        new Http2ServerUpgradeCodec(new Http2Codec(true /* server */, initializer, initialSettings)) {
          override def upgradeTo(ctx: ChannelHandlerContext, upgradeRequest: FullHttpRequest) {
            upgradeCounter.incr()
            // we turn off backpressure because Http2 only works with autoread on for now
            ctx.channel.config.setAutoRead(true)
            super.upgradeTo(ctx, upgradeRequest)
          }
        }
      } else null
    }
  }

  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    val maxRequestSize = params[http.param.MaxRequestSize].size
    val httpCodec = p.get(HttpCodecName) match {
      case codec: SourceCodec => codec
      case other => // This is very unexpected. Abort and log very loudly
        p.close()
        val msg = s"Unexpected codec found: ${other.getClass.getSimpleName}. " +
          "Aborting channel initialization"
        val ex = new IllegalStateException(msg)
        Logger.get(this.getClass).error(ex, msg)
        throw ex
    }
    p.addBefore(HttpCodecName, "priorKnowledgeHandler", new PriorKnowledgeHandler(initializer, params))
    p.addAfter(HttpCodecName, "upgradeHandler",
      new HttpServerUpgradeHandler(httpCodec, upgradeCodecFactory, maxRequestSize.inBytes.toInt))

    p.addLast(init)
  }
}
