package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class LLen(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.LLEN
}

case class LIndex(key: Buf, index: Long) extends StrictKeyCommand {
  def name: Buf = Command.LINDEX
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(index.toString))
}

case class LInsert(key: Buf, relativePosition: String, pivot: Buf, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {

  def name: Buf = Command.LINSERT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(relativePosition), pivot, value)
}

case class LPop(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.LPOP
}

case class LPush(key: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(values.nonEmpty, "values must not be empty")
  def name: Buf = Command.LPUSH
  override def body: Seq[Buf] = key +: values
}

case class LRem(key: Buf, count: Long, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {

  def name: Buf = Command.LREM
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(count.toString), value)
}

case class LReset(key: Buf, values: List[Buf], ttl: Long, trim: Long) extends StrictKeyCommand {
  def name: Buf = Command.LRESET
  override def body: Seq[Buf] = key +: Buf.Utf8(trim.toString) +: Buf.Utf8(ttl.toString) +: values
}

case class LSet(key: Buf, index: Long, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {

  def name: Buf = Command.LSET
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(index.toString), value)
}

case class LRange(key: Buf, start: Long, end: Long) extends ListRangeCommand {
  def name: Buf = Command.LRANGE
}

case class RPop(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.RPOP
}

case class RPush(key: Buf, values: List[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.RPUSH
  override def body: Seq[Buf] = key +: values
}

case class LTrim(key: Buf, start: Long, end: Long) extends ListRangeCommand {
  def name: Buf = Command.LTRIM
}

case class RPopLPush(source: Buf, destination: Buf) extends MoveCommand {
  def name: Buf = Command.RPOPLPUSH
}

trait ListRangeCommand extends StrictKeyCommand {
  def start: Long
  def end: Long

  override def body: Seq[Buf] = Seq(key, Buf.Utf8(start.toString), Buf.Utf8(end.toString))
}
