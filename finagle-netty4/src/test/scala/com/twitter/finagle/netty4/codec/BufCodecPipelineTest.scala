package com.twitter.finagle.netty4.codec

import io.netty.channel.{Channel, ChannelHandler, ChannelPipeline}
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.funsuite.AnyFunSuite

class BufCodecPipelineTest extends AnyFunSuite {

  test("BufCodecPipeline adds BufCodec to Channel Pipeline") {
    val channel: Channel = new EmbeddedChannel
    val pipeline: ChannelPipeline = channel.pipeline
    val preCheck: Option[ChannelHandler] = Option(pipeline.get(BufCodec.Key))
    assert(preCheck.isEmpty)

    // Add BufCodec to Pipeline
    BufCodecPipeline(pipeline)

    val postCheck: Option[ChannelHandler] = Option(pipeline.get(BufCodec.Key))
    assert(postCheck.isDefined)
    assert(postCheck.exists(_ == BufCodec))
  }

}
