package com.twitter.finagle.memcached.protocol.text.transport

import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.text.{
  ByteReaderDecoderHandler,
  FrameDecoder,
  FramingDecoder
}
import com.twitter.finagle.memcached.protocol.text.client.MemcachedClientDecoder
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.decoder.DecoderHandler
import com.twitter.finagle.netty4.encoder.BufEncoder
import io.netty.channel._

/**
 * Memcached server framer using netty4 pipelines.
 */
private[finagle] object Netty4ServerFramer extends (ChannelPipeline => Unit) {
  import com.twitter.finagle.memcached.protocol.text.server.ServerFramer
  import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("endec", BufCodec)
    pipeline.addLast("framer", new DecoderHandler(new ServerFramer(StorageCommands)))
  }
}

/**
 * Memcached client framer using netty4 pipelines.
 */
private[finagle] abstract class Netty4ClientFramer[T] extends (ChannelPipeline => Unit) {
  protected def newClientDecoder(): FrameDecoder[T]

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("encoder", BufEncoder)
    pipeline.addLast(
      "framingDecoder",
      new ByteReaderDecoderHandler(new FramingDecoder(newClientDecoder()))
    )
  }
}

private[finagle] object MemcachedNetty4ClientFramer extends Netty4ClientFramer[Response] {
  protected def newClientDecoder: FrameDecoder[Response] = new MemcachedClientDecoder
}
