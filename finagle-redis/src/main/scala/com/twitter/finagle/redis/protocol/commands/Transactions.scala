package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case object Discard extends Command {
  def command: String = Commands.DISCARD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.DISCARD))
}

case object Exec extends Command {
  def command: String = Commands.EXEC
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.EXEC))
}

case object Multi extends Command {
  def command: String = Commands.MULTI
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.MULTI))
}

case object UnWatch extends Command {
  def command: String = Commands.UNWATCH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.UNWATCH))
}

case class Watch(keys: Seq[Buf]) extends KeysCommand {
  def command: String = Commands.WATCH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.WATCH +: keys)
}
object Watch {
  def apply(args: => Seq[Array[Byte]]): Watch = new Watch(args.map(Buf.ByteArray.Owned(_)))
}
