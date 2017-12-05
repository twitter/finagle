package com.twitter.finagle.memcached.protocol.text.transport

import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.decoder.DecoderHandler
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
