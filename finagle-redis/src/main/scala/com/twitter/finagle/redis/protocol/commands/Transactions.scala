package com.twitter.finagle.redis.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case object Discard extends Command {
  def command = Commands.DISCARD
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.DISCARD))
}

case object Exec extends Command {
  def command = Commands.EXEC
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.EXEC))
}

case object Multi extends Command {
  def command = Commands.MULTI
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.MULTI))
}

case object UnWatch extends Command {
  def command = Commands.UNWATCH
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.UNWATCH))
}

case class Watch(keys: Seq[ChannelBuffer]) extends KeysCommand {
  def command = Commands.WATCH
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.WATCH +: keys)
}
object Watch {
  def apply(args: => Seq[Array[Byte]]) =
    new Watch(args.map(ChannelBuffers.wrappedBuffer(_)))
}
