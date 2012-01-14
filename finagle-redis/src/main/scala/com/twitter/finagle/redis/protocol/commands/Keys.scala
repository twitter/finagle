package com.twitter.finagle.redis
package protocol

import util._

import com.twitter.conversions.time._
import com.twitter.util.Time

import Commands.trimList

/**
 * TODO
 *  - EVAL
 *  - MOVE
 *  - OBJECT
 *  - SORT
 */

case class Del(keys: List[String]) extends StrictKeysCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(Commands.DEL +: keys)
}
object Del {
  def apply(key: String) = new Del(List(key))
}

case class Exists(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.EXISTS, key))
}
object Exists {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "EXISTS")
    new Exists(BytesToString(list(0)))
  }
}

case class Expire(key: String, seconds: Long) extends StrictKeyCommand {
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.EXPIRE, key, seconds.toString))
}
object Expire {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIRE")
    RequireClientProtocol.safe {
      new Expire(BytesToString(list(0)), NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class ExpireAt(key: String, timestamp: Time) extends StrictKeyCommand {
  RequireClientProtocol(
    timestamp != null && timestamp > Time.now,
    "Timestamp must be in the future")

  val seconds = timestamp.inSeconds

  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.EXPIREAT, key, seconds.toString))
}
object ExpireAt {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIREAT")
    val secondsString = BytesToString(list(1))
    val seconds = RequireClientProtocol.safe {
      Time.fromSeconds(NumberFormat.toInt(secondsString))
    }
    new ExpireAt(BytesToString(list(0)), seconds)
  }
}

case class Keys(pattern: String) extends Command {
  RequireClientProtocol(pattern != null && pattern.length > 0, "Pattern must be specified")
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.KEYS, pattern))
}
object Keys {
  def apply(args: List[Array[Byte]]) = new Keys(BytesToString.fromList(args).mkString)
}

case class Persist(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.PERSIST, key))
}
object Persist {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "PERSIST")
    new Persist(BytesToString(list(0)))
  }
}

case class Randomkey() extends Command {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.RANDOMKEY))
}

case class Rename(key: String, newkey: String) extends StrictKeyCommand {
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.RENAME, key, newkey))
}
object Rename {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAME")
    new Rename(BytesToString(list(0)), BytesToString(list(1)))
  }
}

case class RenameNx(key: String, newkey: String) extends StrictKeyCommand {
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.RENAMENX, key, newkey))
}
object RenameNx {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAMENX")
    new RenameNx(BytesToString(list(0)), BytesToString(list(1)))
  }
}

case class Ttl(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.TTL, key))
}
object Ttl {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "TTL")
    new Ttl(BytesToString(list(0)))
  }
}

case class Type(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.TYPE, key))
}
object Type {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 1, "TYPE")
    new Type(BytesToString(list(0)))
  }
}
