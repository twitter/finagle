package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util.BytesToString
import com.twitter.finagle.redis.util.NumberFormat
import com.twitter.finagle.redis.util.StringToBytes
import Commands.trimList
import com.twitter.finagle.redis.ClientError

trait StringMonad {
  def getArg(args: List[Array[Byte]], command: String) = {
    BytesToString(trimList(args, 1, command)(0))
  }
}

case class LLen(key: String) extends StrictKeyCommand {
  val command = Commands.LLEN
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(Commands.LLEN, key)))
}

object LLen extends StringMonad {
  def apply(args: List[Array[Byte]]): LLen = {
    LLen(getArg(args, Commands.LLEN))
  }
}

case class LIndex(key: String, index: Int) extends StrictKeyCommand {
  val command = Commands.LINDEX
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, index.toString)))
}

object LIndex {
  def apply(args: List[Array[Byte]]): LIndex = {
    val list = BytesToString.fromList(trimList(args, 2, Commands.LINDEX))
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(list(1))
    }
    LIndex(list(0), index)
  }
}

case class LInsert(key: String, relativePosition: String,
    pivot: Array[Byte], value: Array[Byte]) extends StrictKeyCommand with
    StrictValueCommand {
  val command = Commands.LINSERT
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, relativePosition,
        BytesToString(pivot), BytesToString(value))))
}

object LInsert {
  def apply(args: List[Array[Byte]]): LInsert = {
    val list = trimList(args, 4, Commands.LINSERT)
    LInsert(BytesToString(list(0)), BytesToString(list(1)), list(2), list(3))
  }
}

case class LPop(key: String) extends StrictKeyCommand {
  val command = Commands.LPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key)))
}

object LPop extends StringMonad {
  def apply(args: List[Array[Byte]]): LPop = {
    LPop(getArg(args, Commands.LPOP))
  }
}

case class LPush(key: String, values: List[Array[Byte]]) extends StrictKeyCommand {
  val command = Commands.LPUSH
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes(command) :: StringToBytes(key) :: values)
}

object LPush {
  def apply(args: List[Array[Byte]]): LPush = args match {
    case head :: tail => LPush(BytesToString(head), tail)
    case _ => throw ClientError("Invalid use of LPush")
  }
}

case class LRem(key: String, count: Int, value: Array[Byte]) extends StrictKeyCommand with
  StrictValueCommand {
  val command = Commands.LREM
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, count.toString, BytesToString(value))))
}

object LRem {
  def apply(args: List[Array[Byte]]): LRem = {
    val list = BytesToString.fromList(trimList(args, 3, Commands.LREM))
    val count = RequireClientProtocol.safe {
      NumberFormat.toInt(list(1))
    }
    LRem(list(0), count, StringToBytes(list(2)))
  }
}

case class LSet(key: String, index: Int, value: Array[Byte]) extends StrictKeyCommand with
  StrictValueCommand {
  val command = Commands.LSET
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, index.toString, BytesToString(value))))
}

object LSet {
  def apply(args: List[Array[Byte]]): LSet = {
    val list = BytesToString.fromList(trimList(args, 3, Commands.LSET))
    val index = RequireClientProtocol.safe {
      NumberFormat.toInt(list(1))
    }
    LSet(list(0), index, StringToBytes(list(2)))
  }
}

case class LRange(key: String, start: Int, end: Int) extends StrictKeyCommand {
  val command = Commands.LRANGE
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, start.toString, end.toString)))
}

object LRange {
  def apply(args: List[Array[Byte]]): LRange = {
    val list = BytesToString.fromList(trimList(args, 3, Commands.LRANGE))
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(list(1)), NumberFormat.toInt(list(2)))
    }
    LRange(list(0), start, end)
  }
}


case class RPop(key: String) extends StrictKeyCommand {
  val command = Commands.RPOP
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key)))
}

object RPop extends StringMonad {
  def apply(args: List[Array[Byte]]): RPop = {
    RPop(getArg(args, Commands.RPOP))
  }
}

case class RPush(key: String, values: List[Array[Byte]]) extends StrictKeyCommand {
  val command = Commands.RPUSH
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes(command) :: StringToBytes(key) :: values)
}

object RPush {
  def apply(args: List[Array[Byte]]): RPush = args match {
    case head :: tail => RPush(BytesToString(head), tail)
    case _ => throw ClientError("Invalid use of RPush")
  }
}

case class LTrim(key: String, start: Int, end: Int) extends StrictKeyCommand {
  val command = Commands.LTRIM
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(command, key, start.toString, end.toString)))
}

object LTrim {
  def apply(args: List[Array[Byte]]): LTrim = {
    val list = BytesToString.fromList(trimList(args, 3, Commands.LTRIM))
    val (start, end) = RequireClientProtocol.safe {
      Tuple2(NumberFormat.toInt(list(1)), NumberFormat.toInt(list(2)))
    }
    LTrim(list(0), start, end)
  }
}
