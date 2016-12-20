package com.twitter.finagle.redis.protocol

import com.twitter.finagle.Failure
import com.twitter.finagle.netty3.codec.BufCodec
import com.twitter.io.Buf
import org.jboss.netty.channel._

private[finagle] object Netty3 {

  private[this] class RedisCodec extends SimpleChannelHandler {

    private[this] val decoder = new StageDecoder(Reply.decode)

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
        e.getMessage match {
          case c: Command =>
            Channels.write(ctx, e.getFuture, Command.encode(c))
          case other =>
            e.getFuture.setFailure(Failure(
              s"unexpected type ${other.getClass.getSimpleName} when encoding Command"))
        }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
      e.getMessage match {
        case b: Buf =>
          var reply = decoder.absorb(b)
          while (reply != null) {
            Channels.fireMessageReceived(ctx, reply)
            reply = decoder.absorb(Buf.Empty)
          }
        case other => Channels.fireExceptionCaught(ctx, Failure(
          s"unexpected type ${other.getClass.getSimpleName} when decoding Reply"))
      }
  }

  val Codec: ChannelPipelineFactory = new ChannelPipelineFactory {
    override def getPipeline: ChannelPipeline =  {
      val pipeline = Channels.pipeline()

      pipeline.addLast("bufCodec", new BufCodec)
      pipeline.addLast("redisCodec", new RedisCodec)

      pipeline
    }
  }
}