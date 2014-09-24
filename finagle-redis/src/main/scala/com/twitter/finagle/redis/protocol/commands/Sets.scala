package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util.GetMonadArg
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util.StringToChannelBuffer
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class SAdd(key: ChannelBuffer, values: Seq[ChannelBuffer]) extends StrictKeyCommand {
  val command = Commands.SADD
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SADD, key) ++ values)
}

object SAdd {
  def apply(args: Seq[Array[Byte]]): SAdd = args match {
    case head :: tail =>
      SAdd(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)
    case _ =>
      throw ClientError("Invalid use of SAdd")
  }
}

case class SMembers(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.SMEMBERS
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SMEMBERS, key))
}

object SMembers {
  def apply(args: Seq[Array[Byte]]): SMembers = {
    SMembers(GetMonadArg(args, CommandBytes.SMEMBERS))
  }
}

case class SIsMember(key: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.SISMEMBER
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SISMEMBER, key, value))
}

object SIsMember {
  def apply(args: Seq[Array[Byte]]): SIsMember = {
    val list = Commands.trimList(args, 2, Commands.SISMEMBER)
    SIsMember(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class SCard(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.SCARD
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SCARD, key))
}

object SCard {
  def apply(args: Seq[Array[Byte]]): SCard =
    SCard(GetMonadArg(args, CommandBytes.SCARD))
}

case class SRem(key: ChannelBuffer, values: List[ChannelBuffer]) extends StrictKeyCommand {
  val command = Commands.SREM
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SREM, key) ++ values)
}

object SRem {
  def apply(args: Seq[Array[Byte]]): SRem = args match {
    case head :: tail =>
      SRem(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)
    case _ => throw ClientError("Invalid use of SRem")
  }
}

case class SPop(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.SPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SPOP, key))
}

object SPop {
  def apply(args: Seq[Array[Byte]]): SPop = SPop(GetMonadArg(args, CommandBytes.SPOP))
}

case class SRandMember(key: ChannelBuffer, count: Option[Int] = None) extends StrictKeyCommand {
  val command = Commands.SRANDMEMBER
  override def toChannelBuffer = {
    val commands = Seq(CommandBytes.SRANDMEMBER, key) ++
      count.map(c => StringToChannelBuffer(c.toString))
    RedisCodec.toUnifiedFormat(commands)
  }
}

object SRandMember {
  def apply(args: Seq[Array[Byte]]): SRandMember =
    SRandMember(GetMonadArg(args, CommandBytes.SRANDMEMBER))
}

case class SInter(keys: Seq[ChannelBuffer]) extends StrictKeysCommand {
  val command = Commands.SINTER
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.SINTER +: keys)
}

object SInter {
  def apply(args: => Seq[Array[Byte]]) =
    new SInter(args.map(ChannelBuffers.wrappedBuffer))
}
