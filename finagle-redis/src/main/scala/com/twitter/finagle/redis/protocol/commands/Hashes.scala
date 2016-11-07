package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import java.lang.{Long => JLong}

case class HDel(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def command: String = Commands.HDEL
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HDEL, key) ++ fields)
}

case class HExists(key: Buf , field: Buf) extends StrictKeyCommand {

  def command: String = Commands.HEXISTS
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HEXISTS, key, field))
}

case class HGet(key: Buf, field: Buf) extends StrictKeyCommand {
  def command: String = Commands.HGET
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HGET, key, field))
}

case class HGetAll(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.HGETALL
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HGETALL, key))
}

case class HIncrBy(key: Buf, field: Buf, amount: Long) extends StrictKeyCommand {
  def command: String = Commands.HINCRBY
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.HINCRBY, key, field, StringToBuf(amount.toString))
  )
}

case class HKeys(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.HKEYS
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HKEYS, key))
}

case class HLen(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.HLEN
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HLEN, key))
}

case class HMGet(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def command: String = Commands.HMGET
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HMGET, key) ++ fields)
}

case class HMSet(key: Buf, fv: Map[Buf, Buf]) extends StrictKeyCommand {
  def command: String  = Commands.HMSET
  val fvList: Seq[Buf] = fv.flatMap { case (f, v) =>
    f :: v :: Nil
  }(collection.breakOut)

  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HMSET, key) ++ fvList)
}

case class HScan(
  key: Buf,
  cursor: Long,
  count: Option[JLong] = None,
  pattern: Option[Buf] = None
) extends Command {
  def command: String  = Commands.HSCAN
  def toBuf: Buf = {
    val bufs = Seq(CommandBytes.HSCAN, key, StringToBuf(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(CommandBytes.COUNT, StringToBuf(count.toString))
      case None        => bufs
    }
    val withPattern = pattern match {
      case Some(pattern) => withCount ++ Seq(CommandBytes.MATCH, pattern)
      case None          => withCount
    }
    RedisCodec.toUnifiedBuf(withPattern)
  }
}

case class HSet(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def command: String  = Commands.HSET
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HSET, key, field, value))
}

case class HSetNx(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def command: String  = Commands.HSETNX
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HSETNX, key, field, value))
}

case class HVals(key: Buf) extends StrictKeyCommand {
  def command: String  = Commands.HVALS
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.HVALS, key))
}
