package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack
import io.netty.channel.{Channel, ChannelPipeline}

/**
 * Channel Initializer which exposes the netty pipeline to the transporter.
 *
 * @param params configuration parameters.
 * @param pipelineInit a callback for initialized pipelines
 */
final private[netty4] class RawNetty4ClientChannelInitializer(
  pipelineInit: ChannelPipeline => Unit,
  params: Stack.Params)
    extends AbstractNetty4ClientChannelInitializer(params) {

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)
    pipelineInit(ch.pipeline)
  }
}
