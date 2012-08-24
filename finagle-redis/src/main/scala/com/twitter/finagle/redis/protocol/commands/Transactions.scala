package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case object Discard extends Command {
  def command = Commands.DISCARD
  val toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.DISCARD))
}

case object Exec extends Command {
  def command = Commands.EXEC
  val toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.EXEC))
}

case object Multi extends Command {
  def command = Commands.MULTI
  val toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.MULTI))
}

case object UnWatch extends Command {
  def command = Commands.UNWATCH
  val toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.UNWATCH))
}

case class Watch(keys: List[ChannelBuffer]) extends KeysCommand {
  def command = Commands.WATCH
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.WATCH +: keys)
}
object Watch {
  def apply(args: => List[Array[Byte]]) =
    new Watch(args.map(ChannelBuffers.wrappedBuffer(_)))
}