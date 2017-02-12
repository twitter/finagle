package com.twitter.finagle.memcached.protocol.text.transport

import com.twitter.finagle.netty4.DirectToHeapInboundHandlerName
import com.twitter.finagle.netty4.channel.DirectToHeapInboundHandler
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.framer.FrameHandler
import io.netty.channel._

/**
 * Memcached server framer using netty4 pipelines.
 */
private[finagle] object Netty4ServerFramer extends (ChannelPipeline => Unit) {
  import com.twitter.finagle.memcached.protocol.text.server.ServerFramer
  import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
    pipeline.addLast("endec", new BufCodec)
    pipeline.addLast("framer", new FrameHandler(new ServerFramer(StorageCommands)))
  }
}

/**
 * Memcached client framer using netty4 pipelines.
 */
private[finagle] object Netty4ClientFramer extends (ChannelPipeline => Unit) {
  import com.twitter.finagle.memcached.protocol.text.client.ClientFramer

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
    pipeline.addLast("endec", new BufCodec)
    pipeline.addLast("framer", new FrameHandler(new ClientFramer))
  }
}