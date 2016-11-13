package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class SAdd(key: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(values.nonEmpty, "values must not be empty")

  def name: Buf = Command.SADD
  override def body: Seq[Buf] = key +: values
}

case class SMembers(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.SMEMBERS
}

case class SIsMember(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def name: Buf = Command.SISMEMBER
  override def body: Seq[Buf] = Seq(key, value)
}

case class SCard(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.SCARD
}

case class SRem(key: Buf, values: List[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.SREM
  override def body: Seq[Buf] = key +: values
}

case class SPop(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.SPOP
}

case class SRandMember(key: Buf, count: Option[Int] = None) extends StrictKeyCommand {
  def name: Buf = Command.SRANDMEMBER
  override def body: Seq[Buf] = key +: count.map(c => Seq(Buf.Utf8(c.toString))).getOrElse(Nil)
}
case class SInter(keys: Seq[Buf]) extends StrictKeysCommand {
  def name: Buf = Command.SINTER
}
