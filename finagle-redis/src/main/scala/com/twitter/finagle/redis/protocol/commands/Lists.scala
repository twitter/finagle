package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import com.twitter.finagle.redis.util._

case class LLen(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.LLEN
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.LLEN, key))
}

case class LIndex(key: Buf, index: Long) extends StrictKeyCommand {
  def command: String = Commands.LINDEX
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.LINDEX, key, StringToBuf(index.toString)))
}

case class LInsert(
    key: Buf,
    relativePosition: String,
    pivot: Buf,
    value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.LINSERT
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.LINSERT, key, StringToBuf(relativePosition), pivot, value)
  )
}

case class LPop(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.LPOP
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.LPOP, key))
}

case class LPush(key: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(values.nonEmpty, "values must not be empty")
  def command: String = Commands.LPUSH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.LPUSH, key) ++ values)
}

case class LRem(key: Buf, count: Long, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.LREM
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.LREM, key, StringToBuf(count.toString), value))
}

case class LSet(key: Buf, index: Long, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.LSET
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.LSET, key, StringToBuf(index.toString), value))
}

case class LRange(key: Buf, start: Long, end: Long) extends ListRangeCommand {
  def command: String = Commands.LRANGE
}

case class RPop(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.RPOP
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.RPOP, key))
}

case class RPush(key: Buf, values: List[Buf]) extends StrictKeyCommand {
  def command: String = Commands.RPUSH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.RPUSH, key) ++ values)
}

case class LTrim(key: Buf, start: Long, end: Long) extends ListRangeCommand {
  def command: String = Commands.LTRIM
}

trait ListRangeCommand extends StrictKeyCommand {
  def start: Long
  def end: Long

  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    StringToBuf(command),
    key,
    StringToBuf(start.toString),
    StringToBuf(end.toString)
  ))
}
