package com.twitter.finagle.redis
package protocol

import util._
import Commands.trimList

case class SAdd(key: String, value: Array[Byte])
    extends StrictKeyCommand
    with StrictValueCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(Commands.SADD), StringToBytes(key), value))
}

object SAdd {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new SAdd(BytesToString(list(0)), list(1))
  }
}

case class SRem(key: String, value: Array[Byte])
    extends StrictKeyCommand
    with StrictValueCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(Commands.SREM), StringToBytes(key), value))
}

object SRem {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new SRem(BytesToString(list(0)), list(1))
  }
}

case class SMembers(key: String) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.SMEMBERS, key))
}

object SMembers {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 1))
    new SMembers(list(0))
  }
}

case class SIsMember(key: String, value: Array[Byte])
    extends StrictKeyCommand
    with StrictValueCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(Commands.SISMEMBER), StringToBytes(key), value))
}

object SIsMember {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new SIsMember(BytesToString(list(0)), list(1))
  }
}