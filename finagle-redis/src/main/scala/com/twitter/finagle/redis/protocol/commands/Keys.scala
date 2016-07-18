package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer
import java.lang.{Long => JLong}

case class Del(bufs: Seq[Buf]) extends StrictKeysCommand {
  def keys: Seq[ChannelBuffer] = bufs.map(ChannelBufferBuf.Owned.extract)
  def command: String = Commands.DEL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.DEL +: bufs)
}

case class Dump(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.DUMP
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.DUMP, buf))
}

case class Exists(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXISTS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.EXISTS, buf))
}

case class Expire(buf: Buf, seconds: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXPIRE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.EXPIRE, buf, StringToBuf(seconds.toString)))
}

case class ExpireAt(buf: Buf, timestamp: Time) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXPIREAT

  val seconds = timestamp.inSeconds

  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.EXPIREAT, buf, StringToBuf(seconds.toString)))
}

case class Keys(pattern: Buf) extends Command {
  def command: String = Commands.KEYS
  RequireClientProtocol(pattern != null && pattern.length > 0, "Pattern must be specified")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.KEYS, pattern))
}

case class Move(buf: Buf, db: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.MOVE
  RequireClientProtocol(db != null && db.length > 0, "Database must be specified")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.MOVE, buf, db))
}

case class Persist(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PERSIST
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PERSIST, buf))
}

case class PExpire(buf: Buf, milliseconds: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PEXPIRE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.PEXPIRE, buf,
      StringToBuf(milliseconds.toString)))
}

case class PExpireAt(buf: Buf, timestamp: Time) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PEXPIREAT

  val milliseconds = timestamp.inMilliseconds

  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PEXPIREAT, buf,
      StringToBuf(milliseconds.toString)))
}

case class PTtl(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PTTL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PTTL, buf))
}

case object Randomkey extends Command {
  def command: String = Commands.RANDOMKEY
  val toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RANDOMKEY))
}

case class Rename(buf: Buf, newkey: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.RENAME
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RENAME, buf, newkey))
}

case class RenameNx(buf: Buf, newkey: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.RENAMENX
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RENAMENX, buf, newkey))
}

case class Scan(cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
  extends Command {

  def command: String = Commands.SCAN
  def toChannelBuffer: ChannelBuffer = {
    val bufs = Seq(CommandBytes.SCAN, StringToBuf(cursor.toString))
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

case class Ttl(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.TTL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.TTL, buf))
}

case class Type(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.TYPE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.TYPE, buf))
}
