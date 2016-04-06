package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.io.Buf
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

case class Watch(bufs: Seq[Buf]) extends KeysCommand {
  override val keys: Seq[ChannelBuffer] = bufs.map(ChannelBufferBuf.Owned.extract(_))
  def command = Commands.WATCH
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.WATCH +: bufs)
}
object Watch {
  def apply(args: => Seq[Array[Byte]]): Watch =
    new Watch(args.map(Buf.ByteArray.Owned(_)))
}
