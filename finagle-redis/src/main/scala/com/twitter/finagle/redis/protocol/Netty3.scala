package com.twitter.finagle.redis.protocol

import com.twitter.finagle.Failure
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.naggati.{Codec => NaggatiCodec}
import com.twitter.io.Buf
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.jboss.netty.channel._


private[finagle] object Netty3 {

  /**
   * This is almost a copy of `BufCodec` except for it does pass-through inbound
   * messages w/o conversion.
   *
   * We can replace it with `BufCodec` once we're able to decode from `Buf`s.
   */
  @Sharable
  private[this] object RedisBufCodec extends SimpleChannelHandler {
    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
      e.getMessage match {
        case b: Buf => Channels.write(ctx, e.getFuture, BufChannelBuffer(b))
        case typ => e.getFuture.setFailure(Failure(
          s"unexpected type ${typ.getClass.getSimpleName} when encoding to ChannelBuffer"))
      }
  }

  val Framer: ChannelPipelineFactory = new ChannelPipelineFactory {
    override def getPipeline: ChannelPipeline =  {
      val pipeline = Channels.pipeline()
      val replyCodec = new ReplyCodec

      pipeline.addLast("buf codec", RedisBufCodec)
      pipeline.addLast("redis codec", new NaggatiCodec(replyCodec.decode, NaggatiCodec.NONE))

      pipeline
    }
  }
}