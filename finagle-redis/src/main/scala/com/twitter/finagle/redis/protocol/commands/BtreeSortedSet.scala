package com.twitter.finagle.redis.protocol.commands

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.Commands._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class BAdd(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command = Commands.BADD

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BADD, keyBuf, field, value))
}

object BAdd {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "BADD")
    new BAdd(
      Buf.ByteArray.Owned(list(0)),
      Buf.ByteArray.Owned(list(1)),
      Buf.ByteArray.Owned(list(2))
    )
  }
}

object BRem {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "BREM requires a hash key and at least one field")

    new BRem(Buf.ByteArray.Owned(args(0)), args.drop(1).map(Buf.ByteArray.Owned.apply))
  }
}
case class BRem(keyBuf: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.BREM
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BREM, keyBuf) ++ fields)
}

object BGet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "BGET")

    new BGet(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
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

object BCard {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "BCARD requires at least one member")

    new BCard(Buf.ByteArray.Owned(args(0)))
  }
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
