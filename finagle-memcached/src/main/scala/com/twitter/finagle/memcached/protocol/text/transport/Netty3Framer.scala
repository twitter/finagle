package com.twitter.finagle.memcached.protocol.text.transport

import com.twitter.finagle.netty3.codec.{FrameDecoderHandler, BufCodec}
import org.jboss.netty.channel._

/**
 * Memcached server framer using netty3 pipelines.
 */
private[finagle] object Netty3ServerFramer extends ChannelPipelineFactory {
  import com.twitter.finagle.memcached.protocol.text.server.ServerFramer
  import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands

  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("endec", new BufCodec)
    pipeline.addLast("framer", new FrameDecoderHandler(new ServerFramer(StorageCommands)))
    pipeline
  }
}

/**
 * Memcached client framer using netty3 pipelines.
 */
private[finagle] object Netty3ClientFramer extends ChannelPipelineFactory {
  import com.twitter.finagle.memcached.protocol.text.client.ClientFramer

  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("endec", new BufCodec)
    pipeline.addLast("framer", new FrameDecoderHandler(new ClientFramer))
    pipeline
  }
}
