package com.twitter.finagle.redis.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import Commands.trimList

case class LLen(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.LLEN
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.LLEN, key))
}

object LLen {
  def apply(args: Seq[Array[Byte]]): LLen = {
    LLen(GetMonadArg(args, CommandBytes.LLEN))
  }
}

case class LIndex(key: ChannelBuffer, index: Long) extends StrictKeyCommand {
  val command = Commands.LINDEX
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.LINDEX, key, StringToChannelBuffer(index.toString)))
}

object LIndex {
  def apply(args: Seq[Array[Byte]]): LIndex = {
    val list = trimList(args, 2, Commands.LINDEX)
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }
    LIndex(ChannelBuffers.wrappedBuffer(list(0)), index)
  }
}

case class LInsert(
    key: ChannelBuffer,
    relativePosition: String,
    pivot: ChannelBuffer,
    value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.LINSERT
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.LINSERT, key, StringToChannelBuffer(relativePosition),
        pivot, value))
}

object LInsert {
  def apply(args: Seq[Array[Byte]]): LInsert = {
    val list = trimList(args, 4, Commands.LINSERT)
    LInsert(ChannelBuffers.wrappedBuffer(list(0)),
      BytesToString(list(1)),
      ChannelBuffers.wrappedBuffer(list(2)),
      ChannelBuffers.wrappedBuffer(list(3)))
  }
}

case class LPop(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.LPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.LPOP, key))
}

object LPop {
  def apply(args: Seq[Array[Byte]]): LPop = {
    LPop(GetMonadArg(args, CommandBytes.LPOP))
  }
}

case class LPush(key: ChannelBuffer, values: Seq[ChannelBuffer]) extends StrictKeyCommand {
  val command = Commands.LPUSH
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.LPUSH, key) ++ values)
}

object LPush {
  def apply(args: List[Array[Byte]]): LPush = args match {
    case head :: tail =>
      LPush(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)
    case _ => throw ClientError("Invalid use of LPush")
  }
}

case class LRem(key: ChannelBuffer, count: Long, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.LREM
  override def toChannelBuffer = {
    val commandArgs = Seq(CommandBytes.LREM, key, StringToChannelBuffer(count.toString), value)
    RedisCodec.toUnifiedFormat(commandArgs)
  }
}

object LRem {
  def apply(args: List[Array[Byte]]): LRem = {
    val list = trimList(args, 3, Commands.LREM)
    val count = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }
    LRem(ChannelBuffers.wrappedBuffer(list(0)), count, ChannelBuffers.wrappedBuffer(list(2)))
  }
}

case class LSet(key: ChannelBuffer, index: Long, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.LSET
  override def toChannelBuffer = {
    val commandArgs = List(CommandBytes.LSET, key, StringToChannelBuffer(index.toString), value)
    RedisCodec.toUnifiedFormat(commandArgs)
  }
}

object LSet {
  def apply(args: List[Array[Byte]]): LSet = {
    val list = trimList(args, 3, Commands.LSET)
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(BytesToString(list(1)))
    }
    LSet(ChannelBuffers.wrappedBuffer(list(0)), index, ChannelBuffers.wrappedBuffer(list(2)))
  }
}

case class LRange(key: ChannelBuffer, start: Long, end: Long) extends ListRangeCommand {
  override val command = Commands.LRANGE
}

object LRange {
  def apply(args: Seq[Array[Byte]]): LRange = {
    val list = trimList(args, 3, Commands.LRANGE)
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(BytesToString(list(1))), NumberFormat.toInt(BytesToString(list(2))))
    }
    LRange(ChannelBuffers.wrappedBuffer(list(0)), start, end)
  }
}

case class RPop(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.RPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.RPOP, key))
}

object RPop extends {
  def apply(args: List[Array[Byte]]): RPop = {
    RPop(GetMonadArg(args, CommandBytes.RPOP))
  }
}

case class RPush(key: ChannelBuffer, values: List[ChannelBuffer]) extends StrictKeyCommand {
  val command = Commands.RPUSH
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.RPUSH, key) ++ values)
}

object RPush {
  def apply(args: List[Array[Byte]]): RPush = args match {
    case head :: tail =>
      RPush(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)
    case _ => throw ClientError("Invalid use of RPush")
  }
}

case class LTrim(key: ChannelBuffer, start: Long, end: Long) extends ListRangeCommand {
  val command = Commands.LTRIM
}

object LTrim {
  def apply(args: Seq[Array[Byte]]): LTrim = {
    val list = trimList(args, 3, Commands.LTRIM)
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(BytesToString(list(1))), NumberFormat.toInt(BytesToString(list(2))))
    }
    LTrim(ChannelBuffers.wrappedBuffer(list(0)), start, end)
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
