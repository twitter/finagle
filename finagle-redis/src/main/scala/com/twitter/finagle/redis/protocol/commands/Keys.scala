package com.twitter.finagle.redis.protocol

import com.twitter.conversions.time._
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.util.Time
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Del(keys: List[ChannelBuffer]) extends StrictKeysCommand {
  def command = Commands.DEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.DEL +: keys)
}
object Del {
  def apply(args: => List[Array[Byte]]) = new Del(args.map(ChannelBuffers.wrappedBuffer(_)))
}

case class Exists(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.EXISTS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.EXISTS, key))
}
object Exists {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "EXISTS")
    new Exists(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Expire(key: ChannelBuffer, seconds: Long) extends StrictKeyCommand {
  def command = Commands.EXPIRE
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(CommandBytes.EXPIRE, key,
      StringToChannelBuffer(seconds.toString)))
}
object Expire {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIRE")
    RequireClientProtocol.safe {
      new Expire(ChannelBuffers.wrappedBuffer(list(0)),
        NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class ExpireAt(key: ChannelBuffer, timestamp: Time) extends StrictKeyCommand {
  def command = Commands.EXPIREAT
  RequireClientProtocol(
    timestamp != null && timestamp > Time.now,
    "Timestamp must be in the future")

  val seconds = timestamp.inSeconds

  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(CommandBytes.EXPIREAT, key,
      StringToChannelBuffer(seconds.toString)))
}
object ExpireAt {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIREAT")
    val secondsString = BytesToString(list(1))
    val seconds = RequireClientProtocol.safe {
      Time.fromSeconds(NumberFormat.toInt(secondsString))
    }
    new ExpireAt(ChannelBuffers.wrappedBuffer(list(0)), seconds)
  }
}

case class Keys(pattern: ChannelBuffer) extends Command {
  def command = Commands.KEYS
  RequireClientProtocol(pattern != null && pattern.readableBytes > 0, "Pattern must be specified")
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.KEYS, pattern))
}
object Keys {
  def apply(args: List[Array[Byte]]) = new Keys(ChannelBuffers.wrappedBuffer(args.head))
}

case class Persist(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.PERSIST
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.PERSIST, key))
}
object Persist {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "PERSIST")
    new Persist(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Randomkey() extends Command {
  def command = Commands.RANDOMKEY
  val toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.RANDOMKEY))
}

case class Rename(key: ChannelBuffer, newkey: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.RENAME
  RequireClientProtocol(newkey != null && newkey.readableBytes > 0, "New key must not be empty")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(CommandBytes.RENAME, key, newkey))
}
object Rename {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAME")
    new Rename(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class RenameNx(key: ChannelBuffer, newkey: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.RENAMENX
  RequireClientProtocol(newkey != null && newkey.readableBytes > 0, "New key must not be empty")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(CommandBytes.RENAMENX, key, newkey))
}
object RenameNx {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAMENX")
    new RenameNx(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class Ttl(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.TTL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.TTL, key))
}
object Ttl {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "TTL")
    new Ttl(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Type(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.TYPE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.TYPE, key))
}
object Type {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "TYPE")
    new Type(ChannelBuffers.wrappedBuffer(list(0)))
  }
}
