package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.netty4.CopyingByteBufByteReader
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * This decoder handler decodes `ByteBuf`s into protocol objects of type `T` using `decoder`.
 *
 * @note `decoder` must add decoded objects to the passed-in `list` parameter. This is an
 *      optimization to avoid creating a list within `decoder` when Netty already provides one for
 *      us to use. `decoder` is expected to be greedy; that is, it decodes the `byteBuf` into as
 *      many protocol objects as possible. `decoder` '''DOES NOT''' own the passed-in reader.
 */
private[memcached] class ByteReaderDecoderHandler[T](decoder: FramingDecoder[T])
    extends ByteToMessageDecoder {

  setSingleDecode(true) // `decoder` is greedy, so no need to call `decode` multiple times.

  def decode(
    channelHandlerContext: ChannelHandlerContext,
    byteBuf: ByteBuf,
    list: java.util.List[AnyRef]
  ): Unit =
    decoder(new CopyingByteBufByteReader(byteBuf), list.asScala.asInstanceOf[mutable.Buffer[T]])
}
