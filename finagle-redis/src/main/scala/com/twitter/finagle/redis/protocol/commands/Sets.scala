package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import java.lang.{Long => JLong}

case class SAdd(key: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(values.nonEmpty, "values must not be empty")

  def name: Buf = Command.SADD
  override def body: Seq[Buf] = key +: values
}

case class SMembers(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.SMEMBERS
}

case class SIsMember(key: Buf, value: Buf) extends StrictKeyCommand with StrictValueCommand {

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

case class SScan(key: Buf, cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
    extends Command {
  def name: Buf = Command.SSCAN
  override def body: Seq[Buf] = {
    val bufs = Seq(key, Buf.Utf8(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(Command.COUNT, Buf.Utf8(count.toString))
      case None => bufs
    }
    pattern match {
      case Some(pattern) => withCount ++ Seq(Command.MATCH, pattern)
      case None => withCount
    }
  }
}
