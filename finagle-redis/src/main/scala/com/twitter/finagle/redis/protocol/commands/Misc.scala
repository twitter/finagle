package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

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

case class Auth(code: ChannelBuffer) extends Command {
  def command = Commands.AUTH
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.AUTH, code))
}

object Auth {
  def apply(code: Seq[Array[Byte]]) = {
    new Auth(ChannelBuffers.wrappedBuffer(code.head))
  }
}

case class Info(section: ChannelBuffer) extends Command {
  def command = Commands.INFO
  def toChannelBuffer = RedisCodec.toUnifiedFormat(section match {
    case ChannelBuffers.EMPTY_BUFFER => Seq(CommandBytes.INFO)
    case _ => Seq(CommandBytes.INFO, section)
  })
}

object Info {
  def apply(section: Seq[Array[Byte]]) = {
      new Info(ChannelBuffers.wrappedBuffer(section.head))
  }
}

case object Quit extends Command {
  def command = Commands.QUIT
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.QUIT))
}
