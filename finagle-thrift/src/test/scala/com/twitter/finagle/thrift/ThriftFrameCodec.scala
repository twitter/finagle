package com.twitter.finagle.thrift

import org.specs.Specification
import org.specs.mock.Mockito

import org.jboss.netty.channel.{ChannelEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder,
                                            LengthFieldPrepender}

object ThriftFrameCodecSpec extends Specification with Mockito {
  "ThriftFrameCodec" should {
    val decoder_ = mock[LengthFieldBasedFrameDecoder]
    val encoder_ = mock[LengthFieldPrepender]
    val codec = new ThriftFrameCodec {
      override val decoder = decoder_
      override val encoder = encoder_
    }
    val ctx = mock[ChannelHandlerContext]
    val e = mock[ChannelEvent]

    "delegate handleUpstream to the decoder" in {
      codec.handleUpstream(ctx, e)
      there was one(decoder_).handleUpstream(ctx, e)
    }

    "delegate handleDownstream to the encoder" in {
      codec.handleDownstream(ctx, e)
      there was one(encoder_).handleDownstream(ctx, e)
    }
  }
}
