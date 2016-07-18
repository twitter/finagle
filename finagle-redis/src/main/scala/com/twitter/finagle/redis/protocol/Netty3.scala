package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.naggati.{Codec => NaggatiCodec}
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, Channels}

/**
 * A [[ChannelPipelineFactory]] that upgrades the Netty 3 pipeline with Redis codec.
 */
private[finagle] object RedisClientPipelineFactory extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    val commandCodec = new CommandCodec
    val replyCodec = new ReplyCodec

    pipeline.addLast("codec", new NaggatiCodec(replyCodec.decode, commandCodec.encode))

    pipeline
  }
}
