package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util.Time
import java.lang.{Long => JLong}

case class Del(keys: Seq[Buf]) extends StrictKeysCommand {
  def command: String = Commands.DEL
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.DEL +: keys)
}

case class Dump(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.DUMP
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.DUMP, key))
}

case class Exists(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.EXISTS
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.EXISTS, key))
}

case class Expire(key: Buf, seconds: Long) extends StrictKeyCommand {
  def command: String = Commands.EXPIRE
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.EXPIRE, key, StringToBuf(seconds.toString)))
}

case class ExpireAt(key: Buf, timestamp: Time) extends StrictKeyCommand {
  def command: String = Commands.EXPIREAT
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(
      Seq(CommandBytes.EXPIREAT, key, StringToBuf(timestamp.inSeconds.toString)))
}

case class Keys(pattern: Buf) extends Command {
  def command: String = Commands.KEYS
  RequireClientProtocol(pattern != null && pattern.length > 0, "Pattern must be specified")
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.KEYS, pattern))
}

case class Move(key: Buf, db: Buf) extends StrictKeyCommand {
  def command: String = Commands.MOVE
  RequireClientProtocol(db != null && db.length > 0, "Database must be specified")
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.MOVE, key, db))
}

case class Persist(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.PERSIST
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.PERSIST, key))
}

case class PExpire(key: Buf, milliseconds: Long) extends StrictKeyCommand {
  def command: String = Commands.PEXPIRE
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.PEXPIRE, key, StringToBuf(milliseconds.toString)))
}

case class PExpireAt(key: Buf, timestamp: Time) extends StrictKeyCommand {
  def command: String = Commands.PEXPIREAT
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.PEXPIREAT, key,
      StringToBuf(timestamp.inMilliseconds.toString)))
}

case class PTtl(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.PTTL
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.PTTL, key))
}

case object Randomkey extends Command {
  def command: String = Commands.RANDOMKEY
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.RANDOMKEY))
}

case class Rename(key: Buf, newkey: Buf) extends StrictKeyCommand {
  def command: String = Commands.RENAME
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.RENAME, key, newkey))
}

case class RenameNx(key: Buf, newkey: Buf) extends StrictKeyCommand {
  def command: String = Commands.RENAMENX
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.RENAMENX, key, newkey))
}

case class Scan(cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
  extends Command {

  def command: String = Commands.SCAN
  def toBuf: Buf = {
    val bufs = Seq(CommandBytes.SCAN, StringToBuf(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(CommandBytes.COUNT, StringToBuf(count.toString))
      case None        => bufs
    }
    val withPattern = pattern match {
      case Some(pattern) => withCount ++ Seq(CommandBytes.PATTERN, pattern)
      case None          => withCount
    }
    RedisCodec.toUnifiedBuf(withPattern)
  }
}

case class Ttl(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.TTL
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.TTL, key))
}

case class Type(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.TYPE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.TYPE, key))
}
