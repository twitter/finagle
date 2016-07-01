package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import Commands.trimList

case class LLen(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LLEN
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LLEN, keyBuf))
}

object LLen {
  def apply(args: Seq[Array[Byte]]): LLen = {
    LLen(GetMonadArg(args, CommandBytes.LLEN))
  }
}

case class LIndex(keyBuf: Buf, index: Long) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LINDEX
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LINDEX, keyBuf, StringToBuf(index.toString)))
}

object LIndex {
  def apply(args: Seq[Array[Byte]]): LIndex = {
    val list = trimList(args, 2, Commands.LINDEX)
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }

    LIndex(Buf.ByteArray.Owned(list(0)), index)
  }
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

object LInsert {
  def apply(args: Seq[Array[Byte]]): LInsert = {
    val list = trimList(args, 4, Commands.LINSERT)

    LInsert(
      Buf.ByteArray.Owned(list(0)),
      BytesToString(list(1)),
      Buf.ByteArray.Owned(list(2)),
      Buf.ByteArray.Owned(list(3))
    )
  }
}

case class LPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LPOP, keyBuf))
}

object LPop {
  def apply(args: Seq[Array[Byte]]): LPop = {
    LPop(GetMonadArg(args, CommandBytes.LPOP))
  }
}

case class LPush(keyBuf: Buf, values: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.LPUSH
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.LPUSH, keyBuf) ++ values)
}

object LPush {
  def apply(args: List[Array[Byte]]): LPush = args match {
    case head :: tail =>
      LPush(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))
    case _ => throw ClientError("Invalid use of LPush")
  }
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

object LRem {
  def apply(args: List[Array[Byte]]): LRem = {
    val list = trimList(args, 3, Commands.LREM)
    val count = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }

    LRem(Buf.ByteArray.Owned(list(0)), count, Buf.ByteArray.Owned(list(2)))
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

object LSet {
  def apply(args: List[Array[Byte]]): LSet = {
    val list = trimList(args, 3, Commands.LSET)
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }

    LSet(Buf.ByteArray.Owned(list(0)), index, Buf.ByteArray.Owned(list(2)))
  }
}

case class LRange(keyBuf: Buf, start: Long, end: Long) extends ListRangeCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  override val command = Commands.LRANGE
}

object LRange {
  def apply(args: Seq[Array[Byte]]): LRange = {
    val list = trimList(args, 3, Commands.LRANGE)
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(BytesToString(list(1))), NumberFormat.toInt(BytesToString(list(2))))
    }
    LRange(Buf.ByteArray.Owned(list(0)), start, end)
  }
}

case class RPop(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command = Commands.RPOP
  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RPOP, keyBuf))
}

object RPop extends {
  def apply(args: List[Array[Byte]]): RPop = {
    RPop(GetMonadArg(args, CommandBytes.RPOP))
  }
}

case class RPush(keyBuf: Buf, values: List[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.RPUSH

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RPUSH, keyBuf) ++ values)
}

object RPush {
  def apply(args: List[Array[Byte]]): RPush = args match {
    case head :: tail =>
      RPush(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))
    case _ => throw ClientError("Invalid use of RPush")
  }
}

case class LTrim(keyBuf: Buf, start: Long, end: Long) extends ListRangeCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command = Commands.LTRIM
}

object LTrim {
  def apply(args: Seq[Array[Byte]]): LTrim = {
    val list = trimList(args, 3, Commands.LTRIM)
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(BytesToString(list(1))), NumberFormat.toInt(BytesToString(list(2))))
    }

    LTrim(Buf.ByteArray.Owned(list(0)), start, end)
  }
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
