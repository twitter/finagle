package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class SAdd(key: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(values.nonEmpty, "values must not be empty")

  def command: String = Commands.SADD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SADD, key) ++ values)
}

case class SMembers(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.SMEMBERS
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SMEMBERS, key))
}

case class SIsMember(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.SISMEMBER
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SISMEMBER, key, value))
}

case class SCard(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.SCARD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SCARD, key))
}

case class SRem(key: Buf, values: List[Buf]) extends StrictKeyCommand {
  def command: String = Commands.SREM
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SREM, key) ++ values)
}

case class SPop(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.SPOP
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SPOP, key))
}

case class SRandMember(key: Buf, count: Option[Int] = None) extends StrictKeyCommand {
  def command: String = Commands.SRANDMEMBER
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.SRANDMEMBER, key) ++ count.map(c => Buf.Utf8(c.toString)
  ))
}
case class SInter(keys: Seq[Buf]) extends StrictKeysCommand {
  def command: String = Commands.SINTER
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.SINTER +: keys)
}
