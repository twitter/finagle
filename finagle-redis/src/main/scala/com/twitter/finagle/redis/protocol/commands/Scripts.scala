package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Eval(
    script: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer],
    args: Seq[ChannelBuffer])
  extends KeysCommand
{
  def command = Commands.EVAL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
      CommandBytes.EVAL,
      script,
      StringToChannelBuffer(numkeys.toString)) ++ keys ++ args)
}

object Eval {
  def apply(args: Seq[Array[Byte]]): Eval = {
    val list = trimList(args, 3, Commands.EVAL)
    val script = ChannelBuffers.wrappedBuffer(args(0))
    val numkeys = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }
    Eval(ChannelBuffers.wrappedBuffer(args(0)),
      numkeys,
      Seq(ChannelBuffers.wrappedBuffer(list(2))),
      Seq(ChannelBuffers.wrappedBuffer(list(3))))
  }
}

case class EvalSha(script: ChannelBuffer) extends KeysCommand {
  def command = Commands.EVALSHA
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EVALSHA, script))
}

object EvalSha {
  def apply(args: Seq[Array[Byte]]): EvalSha = {
    val list = trimList(args, 1, Commands.EVALSHA)
    EvalSha(ChannelBuffers.wrappedBuffer(list(0)))
  }
}
