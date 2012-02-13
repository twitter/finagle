package com.twitter.finagle.redis.protocol

import Commands.trimList
import com.twitter.finagle.redis.util._

case class FlushDB extends Command {
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.FLUSHDB))
}

object Select {
  def apply(args: List[Array[Byte]]) = {
    val index = BytesToString.fromList(trimList(args, 1, "SELECT"))(1).toInt
    new Select(index)
  }
}

case class Select(index: Int) extends Command {
  def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.SELECT, index.toString))
}