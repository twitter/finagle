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

case class BRange(key: Buf, count: Buf, startField: Option[Buf], endField: Option[Buf])
    extends StrictKeyCommand {
  def name: Buf = Command.BRANGE

  override def body: Seq[Buf] = (startField, endField) match {
    case (Some(s), Some(e)) => Seq(key, count, Buf.Utf8("startend"), s, e)
    case (None, Some(e)) => Seq(key, count, Buf.Utf8("end"), e)
    case (Some(s), None) => Seq(key, count, Buf.Utf8("start"), s)
    case (None, None) => Seq(key, count)
  }
}

case class BMergeEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long) extends StrictKeyCommand {
  def name: Buf = Command.BMERGEEX
  override def body: Seq[Buf] = {
    val fvList: Seq[Buf] = fv.iterator.flatMap {
      case (f, v) => f :: v :: Nil
    }.toSeq

    key +: (Buf.Utf8(milliseconds.toString) +: fvList)
  }
}
