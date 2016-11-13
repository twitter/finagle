package com.twitter.finagle.redis.protocol.commands

import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf

case class BAdd(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def name: Buf = Command.BADD
  override def body: Seq[Buf] = Seq(key, field, value)
}

case class BRem(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.BREM
  override def body: Seq[Buf] = key +: fields
}

case class BGet(key: Buf, field: Buf) extends StrictKeyCommand {
  def name: Buf = Command.BGET
  override def body: Seq[Buf] = Seq(key, field)
}

case class BCard(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.BCARD
}

case class BRange(key: Buf, startField: Option[Buf], endField: Option[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.BRANGE
  override def body: Seq[Buf] = (startField, endField) match {
    case (Some(s), Some(e)) => Seq(key, Buf.Utf8("startend"), s, e)
    case (None, Some(e)) => Seq(key, Buf.Utf8("end"), e)
    case (Some(s), None) => Seq(key, Buf.Utf8("start"), s)
    case (None, None) => Seq(key)
  }
}

