package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import java.lang.{Long => JLong}

case class HDel(keyBuf: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HDEL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HDEL, keyBuf) ++ fields)
}

case class HExists(keyBuf: Buf , field: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HEXISTS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HEXISTS, keyBuf, field))
}

case class HGet(keyBuf: Buf, field: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HGET
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HGET, keyBuf, field))
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

case class HKeys(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HKEYS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HKEYS, keyBuf))
}

case class HLen(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String = Commands.HLEN
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HLEN, keyBuf))
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

case class HSet(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HSET
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HSET, keyBuf, field, value))
}

case class HSetNx(keyBuf: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HSETNX
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HSETNX, keyBuf, field, value))
}

case class HVals(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command: String  = Commands.HVALS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.HVALS, keyBuf))
}
