package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.redis.util._

case class LLen(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LLEN
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LLEN, keyBuf))
}

case class LIndex(keyBuf: Buf, index: Long) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LINDEX
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LINDEX, keyBuf, StringToBuf(index.toString)))
}

case class LInsert(
    keyBuf: Buf,
    relativePosition: String,
    pivot: Buf,
    valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override def value: ChannelBuffer = BufChannelBuffer(valueBuf)

  val command = Commands.LINSERT
  override def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(
    Seq(CommandBytes.LINSERT, keyBuf, StringToBuf(relativePosition), pivot, valueBuf)
  )
}

case class LPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LPOP, keyBuf))
}

case class LPush(keyBuf: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  RequireClientProtocol(values.nonEmpty, "values must not be empty")

  val command = Commands.LPUSH
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LPUSH, keyBuf) ++ values)
}

case class LRem(keyBuf: Buf, count: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override def value: ChannelBuffer = BufChannelBuffer(valueBuf)

  val command = Commands.LREM
  override def toChannelBuffer = {
    val commandArgs = Seq(CommandBytes.LREM, keyBuf, StringToBuf(count.toString), valueBuf)
    RedisCodec.bufToUnifiedChannelBuffer(commandArgs)
  }
}

case class LSet(keyBuf: Buf, index: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override def value: ChannelBuffer = BufChannelBuffer(valueBuf)

  val command = Commands.LSET
  override def toChannelBuffer = {
    val commandArgs = List(CommandBytes.LSET, keyBuf, StringToBuf(index.toString), valueBuf)
    RedisCodec.bufToUnifiedChannelBuffer(commandArgs)
  }
}

case class LRange(keyBuf: Buf, start: Long, end: Long) extends ListRangeCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override val command = Commands.LRANGE
}

case class RPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.RPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RPOP, keyBuf))
}

case class RPush(keyBuf: Buf, values: List[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.RPUSH

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RPUSH, keyBuf) ++ values)
}

case class LTrim(keyBuf: Buf, start: Long, end: Long) extends ListRangeCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.LTRIM
}

trait ListRangeCommand extends StrictKeyCommand {
  val start: Long
  val end: Long
  val command: String

  override def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(
      Seq(
        StringToChannelBuffer(command),
        key,
        StringToChannelBuffer(start.toString),
        StringToChannelBuffer(end.toString)))
  }
}
