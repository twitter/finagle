package com.twitter.finagle.redis.protocol

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.util.Time
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Del(keys: Seq[ChannelBuffer]) extends StrictKeysCommand {
  def command = Commands.DEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.DEL +: keys)
}
object Del {
  def apply(args: => Seq[Array[Byte]]) = new Del(args.map(ChannelBuffers.wrappedBuffer(_)))
}

case class Dump(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.DUMP
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.DUMP, key))
}
object Dump {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "DUMP")
    new Dump(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Exists(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.EXISTS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.EXISTS, key))
}
object Exists {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "EXISTS")
    new Exists(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Expire(key: ChannelBuffer, seconds: Long) extends StrictKeyCommand {
  def command = Commands.EXPIRE
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EXPIRE, key,
      StringToChannelBuffer(seconds.toString)))
}
object Expire {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "EXPIRE")
    RequireClientProtocol.safe {
      new Expire(ChannelBuffers.wrappedBuffer(list(0)),
        NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class ExpireAt(key: ChannelBuffer, timestamp: Time) extends StrictKeyCommand {
  def command = Commands.EXPIREAT

  val seconds = timestamp.inSeconds

  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EXPIREAT, key,
      StringToChannelBuffer(seconds.toString)))
}
object ExpireAt {
  def apply(args: Seq[Array[Byte]]) = {
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
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.KEYS, pattern))
}
object Keys {
  def apply(args: Seq[Array[Byte]]) = new Keys(ChannelBuffers.wrappedBuffer(args.head))
}

case class Move(key: ChannelBuffer, db: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.MOVE
  RequireClientProtocol(db != null && db.readableBytes > 0, "Database must be specified")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.MOVE, key, db))
}
object Move {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "MOVE")
    new Move(ChannelBuffers.wrappedBuffer(list(0)),
      ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class Persist(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.PERSIST
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PERSIST, key))
}
object Persist {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "PERSIST")
    new Persist(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class PExpire(key: ChannelBuffer, milliseconds: Long) extends StrictKeyCommand {
  def command = Commands.PEXPIRE
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.PEXPIRE, key,
      StringToChannelBuffer(milliseconds.toString)))
}
object PExpire {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "PEXPIRE")
    RequireClientProtocol.safe {
      new PExpire(ChannelBuffers.wrappedBuffer(list(0)),
        NumberFormat.toLong(BytesToString(list(1))))
    }
  }
}

case class PExpireAt(key: ChannelBuffer, timestamp: Time) extends StrictKeyCommand {
  def command = Commands.PEXPIREAT

  val milliseconds = timestamp.inMilliseconds

  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.PEXPIREAT, key,
      StringToChannelBuffer(milliseconds.toString)))
}
object PExpireAt {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "PEXPIREAT")
    val millisecondsString = BytesToString(list(1))
    val milliseconds = RequireClientProtocol.safe {
      Time.fromMilliseconds(NumberFormat.toLong(millisecondsString))
    }
    new PExpireAt(ChannelBuffers.wrappedBuffer(list(0)), milliseconds)
  }
}

case class PTtl(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.PTTL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PTTL, key))
}
object PTtl {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "PTTL")
    new PTtl(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Randomkey() extends Command {
  def command = Commands.RANDOMKEY
  val toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.RANDOMKEY))
}

case class Rename(key: ChannelBuffer, newkey: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.RENAME
  RequireClientProtocol(newkey != null && newkey.readableBytes > 0, "New key must not be empty")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.RENAME, key, newkey))
}
object Rename {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAME")
    new Rename(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class RenameNx(key: ChannelBuffer, newkey: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.RENAMENX
  RequireClientProtocol(newkey != null && newkey.readableBytes > 0, "New key must not be empty")
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.RENAMENX, key, newkey))
}
object RenameNx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "RENAMENX")
    new RenameNx(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class Scan(cursor: Long, count: Option[JLong] = None, pattern: Option[ChannelBuffer] = None)
extends Command {
  def command = Commands.SCAN
  def toChannelBuffer = {
    val bufs = Seq(CommandBytes.SCAN, StringToChannelBuffer(cursor.toString))
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
    val pattern = findPattern(args0, args1).map(StringToChannelBuffer(_))
    new Scan(cursor, count, pattern)
  }
}

case class Ttl(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.TTL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.TTL, key))
}
object Ttl {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "TTL")
    new Ttl(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class Type(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.TYPE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.TYPE, key))
}
object Type {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 1, "TYPE")
    new Type(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

object ScanCompanion {

  def findArgs(args: Seq[String]): (Seq[String], Seq[String]) = {
    args.head.toUpperCase match {
      case Count.COUNT     => args.splitAt(2)
      case Pattern.PATTERN => args.splitAt(2)
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
