package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

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

object Select {
  def apply(index: Seq[Array[Byte]]) = {
    new Select(NumberFormat.toInt(BytesToString(index.head)))
  }
}

case class Auth(code: Buf) extends Command {
  def command = Commands.AUTH
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.AUTH, ChannelBufferBuf.Owned.extract(code)))
}

object Auth {
  def apply(code: Seq[Array[Byte]]): Auth = {
    new Auth(Buf.ByteArray.Owned(code.head))
  }
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

object Info {
  def apply(section: Seq[Array[Byte]]): Info = {
      new Info(section.headOption.map(Buf.ByteArray.Owned(_)).getOrElse(Buf.Empty))
  }
}

case object Ping extends Command {
  def command = Commands.PING
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PING))
}

case object Quit extends Command {
  def command = Commands.QUIT
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.QUIT))
}

case class ConfigSet(param: ChannelBuffer, value: ChannelBuffer) extends Config(ConfigSet.channelBuffer, Seq(param, value))
object ConfigSet extends ConfigHelper {
  val command = "SET"
  def apply(args: Seq[Array[Byte]]): ConfigSet = {
    val list = trimList(args, 2, "CONFIG SET")
    new ConfigSet(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class ConfigGet(param: ChannelBuffer) extends Config(ConfigGet.channelBuffer, Seq(param))
object ConfigGet extends ConfigHelper {
  val command = "GET"
  def apply(args: Seq[Array[Byte]]): ConfigGet = {
    val list = trimList(args, 1, "CONFIG GET")
    new ConfigGet(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class ConfigResetStat() extends Config(sub = ConfigResetStat.channelBuffer, args = Seq())
object ConfigResetStat extends ConfigHelper {
  val command = "RESETSTAT"
  def apply(args: Seq[Array[Byte]]): ConfigResetStat = new ConfigResetStat()
}

abstract class Config(sub: ChannelBuffer, args: Seq[ChannelBuffer]) extends Command {
  def command = Commands.CONFIG
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.CONFIG, sub) ++ args)

}

trait ConfigHelper {
  def command: String
  def apply(args: Seq[Array[Byte]]): Config

  def channelBuffer: ChannelBuffer = StringToChannelBuffer(command)
  def bytes: Array[Byte] = StringToBytes(command)

}

object Config {
  val subCommands: Seq[ConfigHelper] = Seq(ConfigGet,ConfigSet, ConfigResetStat)

  def apply(args: Seq[Array[Byte]]): Config = {
    val subCommandString = new String(trimList(args.headOption.toList, 1, "CONFIG")(0)).toUpperCase
    val subCommand = subCommands.find{_.command == subCommandString}.getOrElse(throw ClientError("Invalid Config command " + subCommandString))
    subCommand(args.tail)
  }
}


case class SlaveOf(host: ChannelBuffer, port: ChannelBuffer) extends Command {
  def command = Commands.SLAVEOF
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.SLAVEOF, host, port))
}

object SlaveOf {
  def apply(args: Seq[Array[Byte]]) = {
    new SlaveOf(ChannelBuffers.wrappedBuffer(args(0)), ChannelBuffers.wrappedBuffer(args(1)))
  }
  val noOne = apply(Seq(StringToBytes("NO"), StringToBytes("ONE")))
}
