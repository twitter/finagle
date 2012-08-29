package com.twitter.finagle.redis
package protocol

import util._
import Commands.trimList

case class RPush(key: String, value: Array[Byte])
    extends StrictKeyCommand
    with StrictValueCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(Commands.RPUSH), StringToBytes(key), value))
}

object RPush {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new RPush(BytesToString(list(0)), list(1))
  }
}

case class LPush(key: String, value: Array[Byte])
    extends StrictKeyCommand
    with StrictValueCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(Commands.LPUSH), StringToBytes(key), value))
}

object LPush {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new LPush(BytesToString(list(0)), list(1))
  }
}

case class LRange(key: String, start: Int, end: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.LRANGE, key, start.toString, end.toString))
}
object LRange {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3))
    val start = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val end = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new LRange(list(0), start, end)
  }
}
