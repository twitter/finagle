package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._

case class Discard() extends Command {
  val command = Commands.DISCARD
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.DISCARD))
}

case class Exec() extends Command {
  val command = Commands.EXEC
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.EXEC))
}

case class Multi() extends Command {
  val command = Commands.MULTI
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.MULTI))
}

case class UnWatch() extends Command {
  val command = Commands.UNWATCH
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.UNWATCH))
}

case class Watch(keys: List[Array[Byte]]) extends ByteKeysCommand {
  val command = Commands.WATCH
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(StringToBytes(Commands.WATCH) +: keys)
}

object Watch {
  def apply(key: String) = new Watch(List(StringToBytes(key)))
}