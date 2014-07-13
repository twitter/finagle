package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Eval(script: ChannelBuffer) extends Command {
  def command = Commands.EVAL
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EVAL, script))
}

object Eval {
  def apply(args: Seq[Array[Byte]]): Eval = {
    val list = trimList(args, 1, Commands.EVAL)
    Eval(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class EvalSha(script: ChannelBuffer) extends Command {
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
