package com.twitter.finagle.mux.transport

import com.twitter.finagle.mux.transport.Message.{ReplyStatus, Tags, Types}
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.io.Buf
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}

/**
 * Handler for managing framed inbound Mux buffers. Distinguishes itself from the
 * [[com.twitter.finagle.netty4.channel.AnyToHeapInboundHandler]] by excluding messages
 * which decode to POJOs from copying and copies payload bodies individually to allow for
 * free extraction later which eliminates a copy.
 *
 * @note Must be installed after the LengthFieldBasedFrameDecoder instance.
 */
@Sharable
private object MuxDirectBufferHandler extends ChannelDuplexHandler {

  private def copyRelease(bb: ByteBuf): Buf = {
    val res = new Array[Byte](bb.readableBytes)
    bb.readBytes(res)
    bb.release()
    Buf.ByteArray.Owned(res)
  }

  private def copyRetain(bb: ByteBuf): Buf = {
    val res = new Array[Byte](bb.readableBytes)
    bb.readBytes(res)
    Buf.ByteArray.Owned(res)
  }

  // efficiently decode a ByteBuf representing an rdispatch into a Buf
  private def decodeRdispatch(ctx: ChannelHandlerContext, startIdx: Int, bb: ByteBuf): Buf = bb.readByte() match {
    case ReplyStatus.Ok =>
      // We want to index past contexts. In practice responses
      // don't have headers so we expect `nCtxs` to be zero.
      val nCtxs = bb.readShort()
      var idx = 0
      while (idx < nCtxs) {
        val k = bb.readShort()
        bb.readerIndex(bb.readerIndex() + k)
        val v = bb.readShort()
        bb.readerIndex(bb.readerIndex() + v)
        idx += 1
      }

      // We copy these on heap in case they're backed by pooled or direct buffers.
      val headers = copyRetain(bb.slice(startIdx, bb.readerIndex - startIdx))
      // copying the payload separately gives us a free unwrapping to byte array later
      // by producing a Buf.ByteArray with no offsets.
      val payload = copyRelease(bb.slice(bb.readerIndex, bb.readableBytes))
      headers.concat(payload)
    case _ =>
      new ByteBufAsBuf(bb.readerIndex(startIdx)) // non-okay status, this will decode to POJO
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf =>
      if (bb.readableBytes() < 4) {
        // see @note above, we expect framed messages
        bb.release()
        throw BadMessageException(s"saw message with fewer than 4 readable bytes: $msg")
      }

      val savedIdx = bb.readerIndex

      val header = bb.readInt() // we don't #slice to spare the object allocation
      val typ = Message.Tags.extractType(header)
      val tag = Tags.extractTag(header)

      val res: Buf =
        if (Tags.isFragment(tag)) copyRelease(bb.readerIndex(savedIdx))
        else if (Types.isRefCounted(typ)) new ByteBufAsBuf(bb.readerIndex(savedIdx)) // messages that decode to POJO
        else if (typ == Types.Rdispatch)
          decodeRdispatch(ctx, savedIdx, bb)
        else {
          copyRelease(bb.readerIndex(savedIdx)) // copy everything else
        }

      ctx.fireChannelRead(res)

    case _ =>
      ctx.fireChannelRead(msg)
  }

  override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit =
    BufCodec.write(ctx, msg, p)
}
