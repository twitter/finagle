package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import java.lang.{Long => JLong}

object HDel {
  def apply(args: Seq[Array[Byte]]): Command = {
    RequireClientProtocol(args.length >= 2, "HDEL requires a hash key and at least one field")

    HDel(Buf.ByteArray.Owned(args.head), args.tail.map(Buf.ByteArray.Owned.apply))
  }
}

case class HDel(keyBuf: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HDEL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HDEL, keyBuf) ++ fields)
}

object HExists {
  def apply(args: Seq[Array[Byte]]): Command = {
    val list = trimList(args, 2, "HEXISTS")

    HExists(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}
case class HExists(keyBuf: Buf , field: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HEXISTS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HEXISTS, keyBuf, field))
}

object HGet {
  def apply(args: Seq[Array[Byte]]): Command = {
    val list = trimList(args, 2, "HGET")

    HGet(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}
case class HGet(keyBuf: Buf, field: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HGET
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HGET, keyBuf, field))
}

object HGetAll {
  def apply(args: Seq[Array[Byte]]): Command = {
    RequireClientProtocol(args.nonEmpty, "HGETALL requires a key")

    HGetAll(Buf.ByteArray.Owned(args(0)))
  }
}
case class HGetAll(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HGETALL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HGETALL, keyBuf))
}

case class HIncrBy(keyBuf: Buf, field: Buf, amount: Long) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HINCRBY
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(
    Seq(CommandBytes.HINCRBY, keyBuf, field, StringToBuf(amount.toString))
  )
}
object HIncrBy {
  def apply(args: Seq[Array[Byte]]): Command = {
    val amount = RequireClientProtocol.safe {
      NumberFormat.toLong(BytesToString(args(2)))
    }

    HIncrBy(Buf.ByteArray.Owned(args(0)), Buf.ByteArray.Owned(args(1)), amount)
  }
}

object HKeys {
  def apply(keys: Seq[Array[Byte]]): Command = {
    new HKeys(Buf.ByteArray.Owned(keys.head))
  }
}
case class HKeys(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HKEYS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HKEYS, keyBuf))
}

object HLen {
  def apply(keys: Seq[Array[Byte]]): Command = {
    new HLen(Buf.ByteArray.Owned(keys.head))
  }
}

case class HLen(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HLEN
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HLEN, keyBuf))
}

object HMGet {
  def apply(args: Seq[Array[Byte]]): Command = {
    RequireClientProtocol(args.length >= 2, "HMGET requires a hash key and at least one field")

    HMGet(Buf.ByteArray.Owned(args(0)), args.drop(1).map(Buf.ByteArray.Owned.apply))
  }
}
case class HMGet(keyBuf: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HMGET
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HMGET, keyBuf) ++ fields)
}

case class HMSet(keyBuf: Buf, fv: Map[Buf, Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HMSET
  val fvList: Seq[Buf] = fv.flatMap { case (f, v) =>
    f :: v :: Nil
  }(collection.breakOut)

  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HMSET, keyBuf) ++ fvList)
}

object HMSet {
  def apply(args: Seq[Array[Byte]]): Command = {
    RequireClientProtocol(args.length >= 3, "HMSET requires a hash key and at least one field and value")

    val key = Buf.ByteArray.Owned(args(0))
    val fv = args.drop(1).grouped(2).map {
      case field :: value :: Nil =>
        Buf.ByteArray.Owned(field) -> Buf.ByteArray.Owned(value)
      case _ =>
        throw ClientError("Unexpected uneven pair of elements in HMSET")
    }.toMap

    HMSet(key, fv)
  }
}

case class HScan(
  key: Buf,
  cursor: Long,
  count: Option[JLong] = None,
  pattern: Option[Buf] = None
) extends Command {
  def command: String  = Commands.HSCAN
  def toChannelBuffer: ChannelBuffer = {
    val bufs = Seq(CommandBytes.HSCAN, key, StringToBuf(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(CommandBytes.COUNT, StringToBuf(count.toString))
      case None        => bufs
    }
    val withPattern = pattern match {
      case Some(pattern) => withCount ++ Seq(CommandBytes.PATTERN, pattern)
      case None          => withCount
    }
    RedisCodec.bufToUnifiedChannelBuffer(withPattern)
  }
}
object HScan {
  import ScanCompanion._

  def apply(args: Seq[Array[Byte]]): Command = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "Expected at least 1 arguments for hscan command")
    args match {
      case key :: cursor :: Nil  =>
        HScan(Buf.ByteArray.Owned(key),
          NumberFormat.toLong(BytesToString(cursor)))
      case key :: cursor :: tail =>
        parseArgs(key, NumberFormat.toLong(BytesToString(cursor)), tail)
      case _ => throw ClientError("Unexpected args to hscan command")
    }
  }

  def parseArgs(key: Array[Byte], cursor: Long, args: Seq[Array[Byte]]): HScan = {
    val sArgs = BytesToString.fromList(args)
    val (args0, args1) = findArgs(sArgs)
    RequireClientProtocol(args0.length > 1, "Length of arguments must be > 1")
    val count = findCount(args0, args1)
    val pattern = findPattern(args0, args1).map(StringToBuf.apply)

    HScan(Buf.ByteArray.Owned(key), cursor, count, pattern)
  }
}

object HSet {
  def apply(args: Seq[Array[Byte]]): Command = {
    val list = trimList(args, 3, "HSET")

    HSet(
      Buf.ByteArray.Owned(list(0)),
      Buf.ByteArray.Owned(list(1)),
      Buf.ByteArray.Owned(list(2))
    )
  }
}

case class HSet(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HSET
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HSET, keyBuf, field, value))
}

object HSetNx {
  def apply(args: Seq[Array[Byte]]): Command = {
    val list = trimList(args, 3, "HSETNX")
    new HSetNx(
      Buf.ByteArray.Owned(list(0)),
      Buf.ByteArray.Owned(list(1)),
      Buf.ByteArray.Owned(list(2))
    )
  }
}

case class HSetNx(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HSETNX
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HSETNX, keyBuf, field, value))
}


object HVals {
  def apply(args: Seq[Array[Byte]]): Command = {
    val list = trimList(args, 1, "HSET")
    new HVals(Buf.ByteArray.Owned(list(0)))
  }
}

case class HVals(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HVALS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HVALS, keyBuf))
}
