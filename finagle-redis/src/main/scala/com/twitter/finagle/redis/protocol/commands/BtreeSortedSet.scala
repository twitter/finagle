package com.twitter.finagle.redis.protocol.commands

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class BAdd(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command = Commands.BADD

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BADD, keyBuf, field, value))
}

case class BRem(keyBuf: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.BREM
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BREM, keyBuf) ++ fields)
}

case class BGet(keyBuf: Buf, field: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.BGET
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BGET, keyBuf, field))
}

case class BCard(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.BCARD
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BCARD, keyBuf))
}

case class BRange(keyBuf: Buf, startField: Option[Buf], endField: Option[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command = Commands.BRANGE
  val startEnd: Seq[Buf] = (startField, endField) match {
    case (Some(s), Some(e)) => Seq(Buf.Utf8("startend"), s, e)
    case (None, Some(e)) => Seq(Buf.Utf8("end"), e)
    case (Some(s), None) => Seq(Buf.Utf8("start"), s)
    case (None, None) => Seq.empty
  }

  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BRANGE, keyBuf) ++ startEnd)
}
