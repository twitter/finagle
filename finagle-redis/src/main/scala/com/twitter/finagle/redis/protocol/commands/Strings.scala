package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

case class Append(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.APPEND
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.APPEND, key, value))
}

case class BitCount(
    key: Buf,
    start: Option[Int] = None,
    end: Option[Int] = None)
  extends StrictKeyCommand {

  RequireClientProtocol(start.isEmpty && end.isEmpty ||
    start.isDefined && end.isDefined, "Both start and end must be specified")

  def command: String = Commands.BITCOUNT
  def toBuf: Buf = {
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.BITCOUNT, key) ++
      (start match {
        case Some(i) => Seq(StringToBuf(i.toString))
        case None => Seq.empty
      }) ++ (end match {
        case Some(i) => Seq(StringToBuf(i.toString))
        case None => Seq.empty
      }))
  }
}

case class BitOp(op: Buf, dstKey: Buf, srcKeys: Seq[Buf]) extends Command {
  RequireClientProtocol((op equals BitOp.And) || (op equals BitOp.Or) ||
    (op equals BitOp.Xor) || (op equals BitOp.Not),
    "BITOP supports only AND/OR/XOR/NOT")
  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")
  RequireClientProtocol(!op.equals(BitOp.Not) || srcKeys.size == 1,
    "NOT operation takes only 1 input key")

  def command: String = Commands.BITOP
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.BITOP, op, dstKey) ++ srcKeys)
}

object BitOp {
  val And = StringToBuf("AND")
  val Or = StringToBuf("OR")
  val Xor = StringToBuf("XOR")
  val Not = StringToBuf("NOT")
}

case class Decr(override val key: Buf) extends DecrBy(key, 1) {
  override val command: String = Commands.DECR
  override def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.DECR, key))
}

class DecrBy(val key: Buf, val amount: Long) extends StrictKeyCommand {
  def command: String = Commands.DECRBY
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.DECRBY,
    key,
    StringToBuf(amount.toString)
  ))

  override def toString: String = "DecrBy(%s, %d)".format(key, amount)
  override def equals(other: Any): Boolean = other match {
    case that: DecrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any): Boolean = other.isInstanceOf[DecrBy]
}

object DecrBy {
  def apply(keyBuf: Buf, amount: Long): DecrBy = new DecrBy(keyBuf, amount)
}

case class Get(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.GET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.GET, key))
}

case class GetBit(key: Buf, offset: Int) extends StrictKeyCommand {
  def command: String = Commands.GETBIT
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.GETBIT, key, StringToBuf(offset.toString))
  )
}

case class GetRange(key: Buf, start: Long, end: Long) extends StrictKeyCommand {
  def command: String = Commands.GETRANGE
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.GETRANGE, key,
      StringToBuf(start.toString),
      StringToBuf(end.toString)
    ))
}

case class GetSet(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.GETSET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.GETSET, key, value))
}

case class Incr(override val key: Buf) extends IncrBy(key, 1) {
  override def command: String = Commands.INCR
  override def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.INCR, key))
}

class IncrBy(val key: Buf, val amount: Long) extends StrictKeyCommand {
  def command: String = Commands.INCRBY
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.INCRBY, key, StringToBuf(amount.toString)))
  override def toString: String = "IncrBy(%s, %d)".format(key, amount)
  override def equals(other: Any): Boolean = other match {
    case that: IncrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any): Boolean = other.isInstanceOf[IncrBy]
}
object IncrBy {
  def apply(keyBuf: Buf, amount: Long): IncrBy = new IncrBy(keyBuf, amount)
}

case class MGet(keys: Seq[Buf]) extends StrictKeysCommand {
  def command: String = Commands.MGET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.MGET +: keys)
}

case class MSet(kv: Map[Buf, Buf]) extends MultiSet {
  validate()

  def command: String = Commands.MSET
  def toBuf: Buf = {
    val kvList: Seq[Buf] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.toUnifiedBuf(CommandBytes.MSET +: kvList)
  }
}

case class MSetNx(kv: Map[Buf, Buf]) extends MultiSet {
  validate()

  def command: String = Commands.MSETNX
  def toBuf: Buf = {
    val kvList: Seq[Buf] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.toUnifiedBuf(CommandBytes.MSETNX +: kvList)
  }
}

case class PSetEx(key: Buf, millis: Long, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  RequireClientProtocol(millis > 0, "Milliseconds must be greater than 0")

  def command: String = Commands.PSETEX
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.PSETEX,
    key,
    StringToBuf(millis.toString),
    value
  ))
}

sealed trait TimeToLive
case class InSeconds(seconds: Long) extends TimeToLive
case class InMilliseconds(millis: Long) extends TimeToLive

case class Set(
    key: Buf,
    value: Buf,
    ttl: Option[TimeToLive] = None,
    nx: Boolean = false,
    xx: Boolean = false)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.SET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.SET, key, value) ++
      (ttl match {
        case Some(InSeconds(seconds)) =>
          Seq(Set.ExBytes, StringToBuf(seconds.toString))
        case Some(InMilliseconds(millis)) =>
          Seq(Set.PxBytes, StringToBuf(millis.toString))
        case _ => Seq()
      }) ++ (if (nx) Seq(Set.NxBytes) else Seq()) ++
        (if (xx) Seq(Set.XxBytes) else Seq())
  )
}
object Set {
  private val Ex = "EX"
  private val Px = "PX"
  private val Nx = "NX"
  private val Xx = "XX"

  private val ExBytes = StringToBuf(Ex)
  private val PxBytes = StringToBuf(Px)
  private val NxBytes = StringToBuf(Nx)
  private val XxBytes = StringToBuf(Xx)
}

case class SetBit(key: Buf, offset: Int, value: Int) extends StrictKeyCommand {
  def command: String = Commands.SETBIT
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.SETBIT, key,
      StringToBuf(offset.toString),
      StringToBuf(value.toString)))
}

case class SetEx(key: Buf, seconds: Long, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")

  def command: String = Commands.SETEX
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.SETEX,
    key,
    StringToBuf(seconds.toString),
    value
  ))
}

case class SetNx(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.SETNX
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.SETNX, key, value))
}

case class SetRange(key: Buf, offset: Int, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.SETRANGE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.SETRANGE,
    key,
    StringToBuf(offset.toString),
    value
  ))
}

case class Strlen(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.STRLEN
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.STRLEN, key))
}

trait MultiSet extends KeysCommand {
  def kv: Map[Buf, Buf]
  def keys: Seq[Buf] = kv.keys.toSeq
}