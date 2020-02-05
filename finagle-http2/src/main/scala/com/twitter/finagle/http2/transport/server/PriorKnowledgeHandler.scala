package com.twitter.finagle.http2.transport.server

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.{Http2PipelineInitializer, MultiplexHandlerBuilder}
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.param.Stats
import com.twitter.logging.Logger
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.{
  Channel,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelInitializer
}
import io.netty.handler.codec.http2.Http2CodecUtil.connectionPrefaceBuf

private[http2] object PriorKnowledgeHandler {
  private val logger = Logger.get()
}

/**
 * This handler allows an instant upgrade to HTTP/2 if the first bytes received from the client
 * matches the fixed HTTP/2 client preface. If so we upgrade to HTTP/2 right away otherwise we keep
 * running 1.1 with the potential for a cleartext upgrade.
 *
 * In any case, as soon as we either recognize the preface or hit a byte that does not match, we
 * immediately remove our self from the pipeline and pass along any consumed bytes.
 *
 * This handler is stateful and should not be shared!
 */
final private[http2] class PriorKnowledgeHandler(
  initializer: ChannelInitializer[Channel],
  params: Stack.Params)
    extends ChannelInboundHandlerAdapter {

  import PriorKnowledgeHandler._

  val prefaceToRead: ByteBuf = connectionPrefaceBuf
  var bytesConsumed: Integer = 0

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {

    msg match {
      case buf: ByteBuf =>
        val p = ctx.pipeline()

        // We compare bytes as long as they match the preface.
        val prefaceRemaining = prefaceToRead.readableBytes()
        val bytesRead = Math.min(buf.readableBytes(), prefaceRemaining)

        // if what we have read does not match the preface, remove this handler
        // and make sure to send all bytes we have consumed so far downstream.
        if (bytesRead == 0 || !ByteBufUtil.equals(
            buf,
            buf.readerIndex(),
            prefaceToRead,
            prefaceToRead.readerIndex(),
            bytesRead
          )) {
          p.remove(this)
          buf.resetReaderIndex()

          // we consumed bytes (which must have matched the preface) in a previous
          // invocation which we need to pass on as well.
          if (bytesConsumed > 0) {
            ctx.fireChannelRead(connectionPrefaceBuf.slice(0, bytesConsumed))
          }
          ctx.fireChannelRead(msg)
          return
        }

        bytesConsumed += bytesRead
        buf.skipBytes(bytesRead)
        prefaceToRead.skipBytes(bytesRead)

        if (!prefaceToRead.isReadable()) {
          // Entire preface has been read.
          prefaceToRead.release()
          upgradeCounter.incr()

          // we have read a complete preface. Setup HTTP/2 pipeline.
          val (codec, handler) = MultiplexHandlerBuilder.serverFrameCodec(params, initializer)
          MultiplexHandlerBuilder.addStreamsGauge(statsReceiver, codec, ctx.channel)

          p.replace(HttpCodecName, Http2CodecName, codec)
          p.addAfter(Http2CodecName, Http2MultiplexHandlerName, handler)
          p.remove("upgradeHandler")

          Http2PipelineInitializer.setupServerPipeline(p, params)

          // Since we changed the pipeline, our current ctx points to the wrong handler
          // but we can still use this handler as the reference point in the new pipeline
          val nextCtx = p.context(this)
          p.remove(this)

          nextCtx.channel.config.setAutoRead(true)

          // Send new preface downstream as HTTP/2 codec needs it and we ate the original.
          // As the preface might have been matched over several iterations, we cannot
          // just reset reader index on buf and use that.
          nextCtx.fireChannelRead(connectionPrefaceBuf)

          // send any additional bytes left over after the preface was matched.
          nextCtx.fireChannelRead(buf)
        }

      case _ =>
        // Not sure if there are valid cases for this. Allow it for now but log it.

        logger.warning(s"Unexpected non ByteBuf message read: ${msg.getClass.getName} - $msg")
        ctx.fireChannelRead(msg)
    }
  }
}
