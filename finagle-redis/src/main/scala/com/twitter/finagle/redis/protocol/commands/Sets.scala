package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.util.GetMonadArg
import com.twitter.finagle.redis.ClientError
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class SAdd(keyBuf: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.SADD
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SADD, keyBuf) ++ values)
}

object SAdd {
  def apply(args: Seq[Array[Byte]]): SAdd = args match {
    case head :: tail =>
      SAdd(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))
    case _ =>
      throw ClientError("Invalid use of SAdd")
  }
}

case class SMembers(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.SMEMBERS
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SMEMBERS, keyBuf))
}

object SMembers {
  def apply(args: Seq[Array[Byte]]): SMembers = {
    SMembers(GetMonadArg(args, CommandBytes.SMEMBERS))
  }
}

case class SIsMember(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override def value: ChannelBuffer = BufChannelBuffer(valueBuf)

  val command = Commands.SISMEMBER
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SISMEMBER, keyBuf, valueBuf))
}

object SIsMember {
  def apply(args: Seq[Array[Byte]]): SIsMember = {
    val list = Commands.trimList(args, 2, Commands.SISMEMBER)
    SIsMember(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}

case class SCard(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SCARD

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SCARD, keyBuf))
}

object SCard {
  def apply(args: Seq[Array[Byte]]): SCard =
    SCard(GetMonadArg(args, CommandBytes.SCARD))
}

case class SRem(keyBuf: Buf, values: List[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SREM
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SREM, keyBuf) ++ values)
}

object SRem {
  def apply(args: Seq[Array[Byte]]): SRem = args match {
    case head :: tail =>
      SRem(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))
    case _ => throw ClientError("Invalid use of SRem")
  }
}

case class SPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SPOP, keyBuf))
}

object SPop {
  def apply(args: Seq[Array[Byte]]): SPop = SPop(GetMonadArg(args, CommandBytes.SPOP))
}

case class SRandMember(keyBuf: Buf, count: Option[Int] = None) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SRANDMEMBER
  override def toChannelBuffer = {
    val commands = Seq(CommandBytes.SRANDMEMBER, keyBuf) ++
      count.map(c => Buf.Utf8(c.toString))
    RedisCodec.bufToUnifiedChannelBuffer(commands)
  }
}

object SRandMember {
  def apply(args: Seq[Array[Byte]]): SRandMember =
    SRandMember(GetMonadArg(args, CommandBytes.SRANDMEMBER))
}

case class SInter(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  override def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer.apply)

  val command = Commands.SINTER
  override def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.SINTER +: keysBuf)
}

object SInter {
  def apply(args: => Seq[Array[Byte]]) =
    new SInter(args.map(Buf.ByteArray.Owned.apply))
}
