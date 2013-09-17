package com.twitter.finagle.redis.protocol

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

object HDel {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length >= 2, "HDEL requires a hash key and at least one field")
    new HDel(ChannelBuffers.wrappedBuffer(args(0)),
      args.drop(1).map(ChannelBuffers.wrappedBuffer(_)))
  }
}
case class HDel(key: ChannelBuffer, fields: Seq[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.HDEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HDEL, key) ++ fields)
}

object HExists {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "HEXISTS")
    new HExists(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}
case class HExists(key: ChannelBuffer, field: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HEXISTS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HEXISTS, key, field))
}

object HGet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "HGET")
    new HGet(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}
case class HGet(key: ChannelBuffer, field: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HGET, key, field))
}

object HGetAll {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.nonEmpty, "HGETALL requires a key")
    new HGetAll(ChannelBuffers.wrappedBuffer(args(0)))
  }
}
case class HGetAll(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HGETALL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HGETALL, key))
}

case class HIncrBy(key: ChannelBuffer, field: ChannelBuffer, amount: Long) extends StrictKeyCommand {
  def command = Commands.HINCRBY
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HINCRBY, key, field, StringToChannelBuffer(amount.toString)))
}
object HIncrBy {
  def apply(args: Seq[Array[Byte]]): Command = {
    val amount = RequireClientProtocol.safe {
      NumberFormat.toLong(BytesToString(args(2)))
    }
    HIncrBy(ChannelBuffers.wrappedBuffer(args(0)), ChannelBuffers.wrappedBuffer(args(1)), amount)
  }
}

object HKeys {
  def apply(keys: Seq[Array[Byte]]) = {
    new HKeys(ChannelBuffers.wrappedBuffer(keys.head))
  }
}
case class HKeys(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HKEYS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HKEYS, key))
}

object HMGet {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length >= 2, "HMGET requires a hash key and at least one field")
    new HMGet(ChannelBuffers.wrappedBuffer(args(0)),
      args.drop(1).map(ChannelBuffers.wrappedBuffer(_)))
  }
}
case class HMGet(key: ChannelBuffer, fields: Seq[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.HMGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HMGET, key) ++ fields)
}

case class HMSet(key: ChannelBuffer, fv: Map[ChannelBuffer, ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.HMSET

  val fvList: Seq[ChannelBuffer] = fv.flatMap { case(f,v) =>
    f :: v :: Nil
  }(collection.breakOut)

  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HMSET, key) ++ fvList)
}

object HMSet {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length >= 3, "HMSET requires a hash key and at least one field and value")

    val key = ChannelBuffers.wrappedBuffer(args(0))
    val fv = args.drop(1).grouped(2).map {
      case field :: value :: Nil => (ChannelBuffers.wrappedBuffer(field),
        ChannelBuffers.wrappedBuffer(value))
      case _ => throw ClientError("Unexpected uneven pair of elements in HMSET")
    }.toMap

    new HMSet(key, fv)
  }
}

case class HScan(
  key: ChannelBuffer,
  cursor: Long,
  count: Option[JLong] = None,
  pattern: Option[ChannelBuffer] = None
) extends Command {
  def command = Commands.HSCAN
  def toChannelBuffer = {
    val bufs = Seq(CommandBytes.HSCAN, key, StringToChannelBuffer(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(Count.COUNT_CB, StringToChannelBuffer(count.toString))
      case None        => bufs
    }
    val withPattern = pattern match {
      case Some(pattern) => withCount ++ Seq(Pattern.PATTERN_CB, pattern)
      case None          => withCount
    }
    RedisCodec.toUnifiedFormat(withPattern)
  }
}
object HScan {
  import ScanCompanion._

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "Expected at least 1 arguments for hscan command")
    args match {
      case key :: cursor :: Nil  =>
        new HScan(ChannelBuffers.wrappedBuffer(key),
          NumberFormat.toLong(BytesToString(cursor)))
      case key :: cursor :: tail =>
        parseArgs(key, NumberFormat.toLong(BytesToString(cursor)), tail)
      case _ => throw ClientError("Unexpected args to hscan command")
    }
  }

  def parseArgs(key: Array[Byte], cursor: Long, args: Seq[Array[Byte]]) = {
    val sArgs = BytesToString.fromList(args)
    val (args0, args1) = findArgs(sArgs)
    RequireClientProtocol(args0.length > 1, "Length of arguments must be > 1")
    val count = findCount(args0, args1)
    val pattern = findPattern(args0, args1).map(StringToChannelBuffer(_))
    new HScan(ChannelBuffers.wrappedBuffer(key), cursor, count, pattern)
  }
}

object HSet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "HSET")
    new HSet(
      ChannelBuffers.wrappedBuffer(list(0)),
      ChannelBuffers.wrappedBuffer(list(1)),
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}
case class HSet(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand {
  def command = Commands.HSET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HSET, key, field, value))
}

object HSetNx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "HSETNX")
    new HSetNx(
      ChannelBuffers.wrappedBuffer(list(0)),
      ChannelBuffers.wrappedBuffer(list(1)),
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}
case class HSetNx(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand {
  def command = Commands.HSETNX
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HSETNX, key, field, value))
}


object HVals {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "HSET")
    new HVals(ChannelBuffers.wrappedBuffer(list(0)))
  }
}
case class HVals(key: ChannelBuffer)
  extends StrictKeyCommand {
  def command = Commands.HVALS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.HVALS, key))
}
