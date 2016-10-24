package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

case object FlushAll extends Command {
  def command: String = Commands.FLUSHALL
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.FLUSHALL))
}

case object FlushDB extends Command {
  def command: String = Commands.FLUSHDB
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.FLUSHDB))
}

case class Select(index: Int) extends Command {
  def command: String = Commands.SELECT
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.SELECT, StringToBuf(index.toString)))
}

case class Auth(code: Buf) extends Command {
  def command: String = Commands.AUTH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.AUTH, code))
}

case class Info(section: Buf) extends Command {
  def command: String = Commands.INFO
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    section match {
      case Buf.Empty => Seq(CommandBytes.INFO)
      case _ => Seq(CommandBytes.INFO, section)
    }
  )
}

case object Ping extends Command {
  def command: String = Commands.PING
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.PING))
}

case object Quit extends Command {
  def command: String = Commands.QUIT
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.QUIT))
}

case class ConfigSet(
    param: Buf,
    value: Buf)
  extends Config(StringToBuf("SET"), Seq(param, value))

case class ConfigGet(param: Buf) extends Config(StringToBuf("GET"), Seq(param))

case class ConfigResetStat() extends Config(StringToBuf("RESETSTAT"), Seq.empty)

abstract class Config(sub: Buf, args: Seq[Buf]) extends Command {
  def command: String = Commands.CONFIG
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.CONFIG, sub) ++ args)
}

case class SlaveOf(host: Buf, port: Buf) extends Command {
  def command: String = Commands.SLAVEOF
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SLAVEOF, host, port))
}
