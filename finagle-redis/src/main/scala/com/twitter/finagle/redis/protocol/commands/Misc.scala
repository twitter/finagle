package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case object FlushAll extends Command {
  def command = Commands.FLUSHALL
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.FLUSHALL))
}

case object FlushDB extends Command {
  def command = Commands.FLUSHDB
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.FLUSHDB))
}

case class Select(index: Int) extends Command {
  def command = Commands.SELECT
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.SELECT,
    StringToChannelBuffer(index.toString)))
}

case class Auth(code: Buf) extends Command {
  def command = Commands.AUTH
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.AUTH, ChannelBufferBuf.Owned.extract(code)))
}

case class Info(section: Buf) extends Command {
  def command = Commands.INFO
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
    section match {
      case Buf.Empty => Seq(CommandBytes.INFO)
      case _ => Seq(CommandBytes.INFO, ChannelBufferBuf.Owned.extract(section))
    }
  )
}

case object Ping extends Command {
  def command = Commands.PING
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PING))
}

case object Quit extends Command {
  def command = Commands.QUIT
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.QUIT))
}

case class ConfigSet(
    param: ChannelBuffer,
    value: ChannelBuffer)
  extends Config(StringToChannelBuffer("SET"), Seq(param, value))

case class ConfigGet(param: ChannelBuffer) extends Config(StringToChannelBuffer("GET"), Seq(param))

case class ConfigResetStat() extends Config(StringToChannelBuffer("RESETSTAT"), Seq.empty)

abstract class Config(sub: ChannelBuffer, args: Seq[ChannelBuffer]) extends Command {
  def command = Commands.CONFIG
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.CONFIG, sub) ++ args)
}

case class SlaveOf(host: Buf, port: Buf) extends Command {
  def command = Commands.SLAVEOF
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SLAVEOF, host, port))
}
