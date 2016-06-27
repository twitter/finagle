package com.twitter.finagle.redis.protocol

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer

case class Del(bufs: Seq[Buf]) extends StrictKeysCommand {
  def keys: Seq[ChannelBuffer] = bufs.map(ChannelBufferBuf.Owned.extract)
  def command: String = Commands.DEL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.DEL +: bufs)
}
object Del {
  def apply(args: => Seq[Array[Byte]]) = new Del(args.map(Buf.ByteArray.Owned(_)))
}

case class Dump(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.DUMP
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.DUMP, buf))
}
object Dump {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "DUMP")
    new Dump(Buf.ByteArray.Owned(list(0)))
  }
}

case class Exists(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXISTS
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.EXISTS, buf))
}
object Exists {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "EXISTS")
    new Exists(Buf.ByteArray.Owned(list(0)))
  }
}

case class Expire(buf: Buf, seconds: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXPIRE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.EXPIRE, buf, StringToBuf(seconds.toString)))
}
object Expire {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIRE")
    RequireClientProtocol.safe {
      new Expire(Buf.ByteArray.Owned(list(0)), NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class ExpireAt(buf: Buf, timestamp: Time) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.EXPIREAT

  val seconds = timestamp.inSeconds

  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.EXPIREAT, buf, StringToBuf(seconds.toString)))
}
object ExpireAt {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIREAT")
    val secondsString = BytesToString(list(1))
    val seconds = RequireClientProtocol.safe {
      Time.fromSeconds(NumberFormat.toInt(secondsString))
    }
    new ExpireAt(Buf.ByteArray.Owned(list(0)), seconds)
  }
}

case class Keys(pattern: Buf) extends Command {
  def command: String = Commands.KEYS
  RequireClientProtocol(pattern != null && pattern.length > 0, "Pattern must be specified")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.KEYS, pattern))
}
object Keys {
  def apply(args: Seq[Array[Byte]]) = new Keys(Buf.ByteArray.Owned(args.head))
}

case class Move(buf: Buf, db: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.MOVE
  RequireClientProtocol(db != null && db.length > 0, "Database must be specified")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.MOVE, buf, db))
}
object Move {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "MOVE")
    new Move(Buf.ByteArray.Owned(list(0)),
      Buf.ByteArray.Owned(list(1)))
  }
}

case class Persist(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PERSIST
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PERSIST, buf))
}
object Persist {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "PERSIST")
    new Persist(Buf.ByteArray.Owned(list(0)))
  }
}

case class PExpire(buf: Buf, milliseconds: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PEXPIRE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(
      Seq(CommandBytes.PEXPIRE, buf,
      StringToBuf(milliseconds.toString)))
}
object PExpire {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "PEXPIRE")
    RequireClientProtocol.safe {
      new PExpire(Buf.ByteArray.Owned(list(0)),
        NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class PExpireAt(buf: Buf, timestamp: Time) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PEXPIREAT

  val milliseconds = timestamp.inMilliseconds

  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PEXPIREAT, buf,
      StringToBuf(milliseconds.toString)))
}
object PExpireAt {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "PEXPIREAT")
    val millisecondsString = BytesToString(list(1))
    val milliseconds = RequireClientProtocol.safe {
      Time.fromMilliseconds(NumberFormat.toLong(millisecondsString))
    }
    new PExpireAt(Buf.ByteArray.Owned(list(0)), milliseconds)
  }
}

case class PTtl(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.PTTL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PTTL, buf))
}
object PTtl {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "PTTL")
    new PTtl(Buf.ByteArray.Owned(list(0)))
  }
}

case class Randomkey() extends Command {
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
object Rename {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAME")
    new Rename(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}

case class RenameNx(buf: Buf, newkey: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.RENAMENX
  RequireClientProtocol(newkey != null && newkey.length > 0, "New key must not be empty")
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.RENAMENX, buf, newkey))
}
object RenameNx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAMENX")
    new RenameNx(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
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
object Scan {
  import ScanCompanion._

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "Expected at least 1 arguments for scan command")
    args match {
      case cursor :: Nil  => new Scan(NumberFormat.toLong(BytesToString(cursor)))
      case cursor :: tail => parseArgs(NumberFormat.toLong(BytesToString(cursor)), tail)
      case _              => throw new ClientError("Unexpected args to scan command")
    }
  }

  def parseArgs(cursor: Long, args: Seq[Array[Byte]]) = {
    val sArgs = BytesToString.fromList(args)
    val (args0, args1) = findArgs(sArgs)
    RequireClientProtocol(args0.size > 1, "Length of arguments must be > 1")
    val count = findCount(args0, args1)
    val pattern = findPattern(args0, args1).map(StringToBuf(_))
    new Scan(cursor, count, pattern)
  }
}

case class Ttl(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.TTL
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.TTL, buf))
}
object Ttl {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "TTL")
    new Ttl(Buf.ByteArray.Owned(list(0)))
  }
}

case class Type(buf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = ChannelBufferBuf.Owned.extract(buf)
  def command: String = Commands.TYPE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.TYPE, buf))
}
object Type {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "TYPE")
    new Type(Buf.ByteArray.Owned(list(0)))
  }
}

object ScanCompanion {

  def findArgs(args: Seq[String]): (Seq[String], Seq[String]) = {
    args.head.toUpperCase match {
      case Commands.COUNT     => args.splitAt(2)
      case Commands.PATTERN => args.splitAt(2)
      case s => throw ClientError("COUNT or PATTERN argument expected, found %s".format(s))
    }
  }

  def findCount(args0: Seq[String], args1: Seq[String]) = Count(args0) match {
    case None if args1.length > 0 =>
      Count(args1) match {
        case None => throw ClientError("Have additional arguments but unable to process")
        case c => c
      }
    case None => None
    case c => c
  }

  def findPattern(args0: Seq[String], args1: Seq[String]) = Pattern(args0) match {
    case None if args1.length > 0 =>
      Pattern(args1) match {
        case None => throw ClientError("Have additional arguments but unable to process")
        case pattern => pattern
      }
    case None => None
    case pattern => pattern
  }

}
