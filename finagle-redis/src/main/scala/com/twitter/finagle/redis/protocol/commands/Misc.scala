package com.twitter.finagle.redis.protocol

import Commands.trimList
import com.twitter.finagle.redis.util._

case class FlushDB() extends Command {
  val command = Commands.FLUSHDB
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.FLUSHDB))
}

object Select {
  def apply(args: List[Array[Byte]]) = {
    val index = BytesToString.fromList(trimList(args, 1, "SELECT"))(1).toInt
    new Select(index)
  }
}

case class Select(index: Int) extends Command {
  val command = Commands.SELECT
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.SELECT, index.toString))
}

object Auth {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "AUTH")
    new Auth(BytesToString(list(0)))
  }
}

case class Auth(code: String) extends Command {
  val command = Commands.AUTH
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.AUTH, code))
}

case class Quit() extends Command {
  val command = Commands.QUIT
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.QUIT))
}
