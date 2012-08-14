package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util.StringToBytes
import com.twitter.finagle.redis.util.BytesToString
import com.twitter.finagle.redis.ClientError

case class SAdd(key: String, values: List[Array[Byte]]) extends StrictKeyCommand {
  val command = Commands.SADD
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes(command) :: StringToBytes(key) :: values)
}

object SAdd {
  def apply(args: List[Array[Byte]]): SAdd = args match {
    case head :: tail => SAdd(BytesToString(head), tail)
    case _ => throw ClientError("Invalid use of SAdd")
  }
}

case class SMembers(key: String) extends StrictKeyCommand {
  val command = Commands.SMEMBERS
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key)))
}

object SMembers {
  def apply(args: List[Array[Byte]]): SMembers = {
    SMembers(BytesToString.getMonadArg(args, Commands.SMEMBERS))
  }
}

case class SIsMember(key: String, value: Array[Byte])
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.SISMEMBER
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(StringToBytes(command), StringToBytes(key), value))
}

object SIsMember {
  def apply(args: List[Array[Byte]]): SIsMember = {
    val list = Commands.trimList(args, 2, Commands.SISMEMBER)
    SIsMember(BytesToString(list(0)), list(1))
  }
}

case class SCard(key: String) extends StrictKeyCommand {
  val command = Commands.SCARD
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key)))
}

object SCard {
  def apply(args: List[Array[Byte]]): SCard =
    SCard(BytesToString.getMonadArg(args, Commands.SCARD))
}

case class SRem(key: String, values: List[Array[Byte]]) extends StrictKeyCommand {
  val command = Commands.SREM
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes(command) :: StringToBytes(key) :: values)
}

object SRem {
  def apply(args: List[Array[Byte]]): SRem = args match {
    case head :: tail => SRem(BytesToString(head), tail)
    case _ => throw ClientError("Invalid use of SRem")
  }
}

case class SPop(key: String) extends StrictKeyCommand {
  val command = Commands.SPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key)))
}

object SPop {
  def apply(args: List[Array[Byte]]): SPop = SPop(BytesToString.getMonadArg(args, Commands.SPOP))
}
