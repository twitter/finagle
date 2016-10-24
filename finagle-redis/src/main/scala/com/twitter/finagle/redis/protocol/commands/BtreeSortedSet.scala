package com.twitter.finagle.redis.protocol.commands

import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf

case class BAdd(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def command: String = Commands.BADD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BADD, key, field, value))
}

case class BRem(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def command: String = Commands.BREM
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BREM, key) ++ fields)
}

case class BGet(key: Buf, field: Buf) extends StrictKeyCommand {
  def command: String = Commands.BGET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BGET, key, field))
}

case class BCard(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.BCARD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BCARD, key))
}

case class BRange(key: Buf, startField: Option[Buf], endField: Option[Buf]) extends StrictKeyCommand {
  def command: String = Commands.BRANGE
  val startEnd: Seq[Buf] = (startField, endField) match {
    case (Some(s), Some(e)) => Seq(Buf.Utf8("startend"), s, e)
    case (None, Some(e)) => Seq(Buf.Utf8("end"), e)
    case (Some(s), None) => Seq(Buf.Utf8("start"), s)
    case (None, None) => Seq.empty
  }

  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BRANGE, key) ++ startEnd)
}
