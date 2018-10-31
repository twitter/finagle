package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import com.twitter.util.{Duration, Time}
import java.lang.{Long => JLong}
import java.net.InetSocketAddress

case class Del(keys: Seq[Buf]) extends StrictKeysCommand {
  def name: Buf = Command.DEL
}

case class Dump(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.DUMP
}

case class Exists(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.EXISTS
}

case class Expire(key: Buf, seconds: Long) extends StrictKeyCommand {
  def name: Buf = Command.EXPIRE
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(seconds.toString))
}

case class ExpireAt(key: Buf, timestamp: Time) extends StrictKeyCommand {
  def name: Buf = Command.EXPIREAT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(timestamp.inSeconds.toString))
}

case class Keys(pattern: Buf) extends Command {
  RequireClientProtocol(pattern != null && pattern.length > 0, "Pattern must be specified")

  def name: Buf = Command.KEYS
  override def body: Seq[Buf] = Seq(pattern)
}

case class Migrate(addr: InetSocketAddress, keys: Seq[Buf], timeout: Duration) extends Command {
  def name: Buf = Command.MIGRATE
  override def body: Seq[Buf] = {
    val ip = Buf.Utf8(addr.getAddress.getHostAddress)
    val port = Buf.Utf8(addr.getPort.toString)
    Seq(
      ip,
      port,
      Buf.Utf8(""),
      Buf.Utf8("0"),
      Buf.Utf8(timeout.inMilliseconds.toString),
      Command.KEYS
    ) ++ keys
  }
}

case class Move(key: Buf, db: Buf) extends StrictKeyCommand {
  RequireClientProtocol(db != null && db.length > 0, "Database must be specified")

  def name: Buf = Command.MOVE
  override def body: Seq[Buf] = Seq(key, db)
}

case class Persist(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.PERSIST
}

case class PExpire(key: Buf, milliseconds: Long) extends StrictKeyCommand {
  def name: Buf = Command.PEXPIRE
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(milliseconds.toString))
}

case class PExpireAt(key: Buf, timestamp: Time) extends StrictKeyCommand {
  def name: Buf = Command.PEXPIREAT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(timestamp.inMilliseconds.toString))
}

case class PTtl(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.PTTL
}

case object Randomkey extends Command {
  def name: Buf = Command.RANDOMKEY
}

case class Rename(key: Buf, newkey: Buf) extends StrictKeyCommand {
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")

  def name: Buf = Command.RENAME
  override def body: Seq[Buf] = Seq(key, newkey)
}

case class RenameNx(key: Buf, newkey: Buf) extends StrictKeyCommand {
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")

  def name: Buf = Command.RENAMENX
  override def body: Seq[Buf] = Seq(key, newkey)
}

case class Scan(cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
    extends Command {

  def name: Buf = Command.SCAN
  override def body: Seq[Buf] = {
    val bufs = Seq(Buf.Utf8(cursor.toString))

    val withCount = count match {
      case Some(count) => bufs ++ Seq(Command.COUNT, Buf.Utf8(count.toString))
      case None => bufs
    }

    pattern match {
      case Some(pattern) => withCount ++ Seq(Command.MATCH, pattern)
      case None => withCount
    }
  }
}

case class Ttl(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.TTL
}

case class Type(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.TYPE
}
