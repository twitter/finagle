package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class SAdd(keyBuf: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  RequireClientProtocol(values.nonEmpty, "values must not be empty")

  val command = Commands.SADD
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SADD, keyBuf) ++ values)
}

case class SMembers(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.SMEMBERS
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SMEMBERS, keyBuf))
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

case class SCard(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SCARD

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SCARD, keyBuf))
}

case class SRem(keyBuf: Buf, values: List[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SREM
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SREM, keyBuf) ++ values)
}

case class SPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.SPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SPOP, keyBuf))
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
case class SInter(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  override def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer.apply)

  val command = Commands.SINTER
  override def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.SINTER +: keysBuf)
}
