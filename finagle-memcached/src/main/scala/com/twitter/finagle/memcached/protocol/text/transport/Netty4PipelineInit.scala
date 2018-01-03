package com.twitter.finagle.memcached.protocol.text.transport

import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.protocol.text.client.{CommandToBuf, MemcachedClientDecoder}
import com.twitter.finagle.netty4.encoder.BufEncoder
import io.netty.channel._

/**
 * Memcached client netty4 pipeline initializer.
 */
private[finagle] abstract class Netty4ClientPipelineInit[In, Out]
    extends (ChannelPipeline => Unit) {
  protected def newClientEncoder(): MessageEncoder[In]
  protected def newClientDecoder(): FrameDecoder[Out]

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("encoder", BufEncoder)
    pipeline.addLast("messageToBuf", new MessageEncoderHandler(newClientEncoder))
    pipeline.addLast(
      "framingDecoder",
      new ByteReaderDecoderHandler(new FramingDecoder(newClientDecoder()))
    )
  }
}

private[finagle] object MemcachedNetty4ClientPipelineInit
    extends Netty4ClientPipelineInit[Command, Response] {
  protected def newClientDecoder: FrameDecoder[Response] = new MemcachedClientDecoder
  protected def newClientEncoder: MessageEncoder[Command] = new CommandToBuf
}
