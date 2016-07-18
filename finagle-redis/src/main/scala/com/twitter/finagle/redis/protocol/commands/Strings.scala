package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class Append(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  val command: String = Commands.APPEND
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.APPEND, keyBuf, valueBuf))
}

case class BitCount(keyBuf: Buf, start: Option[Int] = None,
    end: Option[Int] = None) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.BITCOUNT
  RequireClientProtocol(start.isEmpty && end.isEmpty ||
    start.isDefined && end.isDefined, "Both start and end must be specified")
  def toChannelBuffer: ChannelBuffer = {
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.BITCOUNT, keyBuf) ++
      (start match {
        case Some(i) => Seq(StringToBuf(i.toString))
        case None => Seq.empty
      }) ++ (end match {
        case Some(i) => Seq(StringToBuf(i.toString))
        case None => Seq.empty
      }))
  }
}

case class BitOp(op: Buf, dstKey: Buf,
    srcKeys: Seq[Buf]) extends Command {
  val command: String = Commands.BITOP
  RequireClientProtocol((op equals BitOp.And) || (op equals BitOp.Or) ||
    (op equals BitOp.Xor) || (op equals BitOp.Not),
    "BITOP supports only AND/OR/XOR/NOT")
  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")
  RequireClientProtocol(!op.equals(BitOp.Not) || srcKeys.size == 1,
    "NOT operation takes only 1 input key")
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.BITOP, op, dstKey) ++ srcKeys)
}

object BitOp {
  val And = StringToBuf("AND")
  val Or = StringToBuf("OR")
  val Xor = StringToBuf("XOR")
  val Not = StringToBuf("NOT")
}

case class Decr(override val keyBuf: Buf) extends DecrBy(keyBuf, 1) {
  override val command: String = Commands.DECR
  override def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.DECR, keyBuf))
}

class DecrBy(val keyBuf: Buf, val amount: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.DECRBY
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
      CommandBytes.DECRBY,
      keyBuf,
      StringToBuf(amount.toString)))
  override def toString = "DecrBy(%s, %d)".format(key, amount)
  override def equals(other: Any) = other match {
    case that: DecrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any) = other.isInstanceOf[DecrBy]
}

object DecrBy {
  def apply(keyBuf: Buf, amount: Long) = {
    new DecrBy(keyBuf, amount)
  }
}

case class Get(keyBuf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.GET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.GET, keyBuf))
}

case class GetBit(keyBuf: Buf, offset: Int) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.GETBIT
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.GETBIT,
    keyBuf, StringToBuf(offset.toString)))
}

case class GetRange(keyBuf: Buf, start: Long, end: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.GETRANGE
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.GETRANGE, keyBuf,
      StringToBuf(start.toString),
      StringToBuf(end.toString)
    ))
}

case class GetSet(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.GETSET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.GETSET, keyBuf, valueBuf))
}

case class Incr(override val keyBuf: Buf) extends IncrBy(keyBuf, 1) {
  override val command: String = Commands.INCR
  override def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.INCR, keyBuf))
}

class IncrBy(val keyBuf: Buf, val amount: Long) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.INCRBY
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.INCRBY, keyBuf,
      StringToBuf(amount.toString)))
  override def toString = "IncrBy(%s, %d)".format(key, amount)
  override def equals(other: Any) = other match {
    case that: IncrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any) = other.isInstanceOf[IncrBy]
}
object IncrBy {
  def apply(keyBuf: Buf, amount: Long) = new IncrBy(keyBuf, amount)
}

case class MGet(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer(_))
  val command: String = Commands.MGET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.MGET +: keysBuf)
}

case class MSet(kv: Map[Buf, Buf]) extends MultiSet {
  validate()
  val command: String = Commands.MSET

  def toChannelBuffer: ChannelBuffer = {
    val kvList: Seq[Buf] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.MSET +: kvList)
  }
}

case class MSetNx(kv: Map[Buf, Buf]) extends MultiSet {
  validate()

  val command: String = Commands.MSETNX

  def toChannelBuffer: ChannelBuffer = {
    val kvList: Seq[Buf] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.MSETNX +: kvList)
  }
}

case class PSetEx(keyBuf: Buf, millis: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)

  val command: String = Commands.PSETEX
  RequireClientProtocol(millis > 0, "Milliseconds must be greater than 0")
  def toChannelBuffer: ChannelBuffer = {
    RedisCodec.bufToUnifiedChannelBuffer(Seq(
      CommandBytes.PSETEX,
      keyBuf,
      StringToBuf(millis.toString),
      valueBuf))
  }
}

sealed trait TimeToLive
case class InSeconds(seconds: Long) extends TimeToLive
case class InMilliseconds(millis: Long) extends TimeToLive

case class Set(
  keyBuf: Buf,
  valueBuf: Buf,
  ttl: Option[TimeToLive] = None,
  nx: Boolean = false,
  xx: Boolean = false)
    extends StrictKeyCommand
    with StrictValueCommand {
  val command: String = Commands.SET
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(
    Seq(CommandBytes.SET, keyBuf, valueBuf) ++
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

case class SetBit(keyBuf: Buf, offset: Int, value: Int) extends StrictKeyCommand {
  val command: String = Commands.SETBIT
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SETBIT, keyBuf,
      StringToBuf(offset.toString),
      StringToBuf(value.toString)))
}

case class SetEx(keyBuf: Buf, seconds: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.SETEX
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.SETEX,
    keyBuf,
    StringToBuf(seconds.toString),
    valueBuf
  ))
}

case class SetNx(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.SETNX
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SETNX, keyBuf, valueBuf))
}

case class SetRange(keyBuf: Buf, offset: Int, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.SETRANGE
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.SETRANGE,
    keyBuf,
    StringToBuf(offset.toString),
    valueBuf
  ))
}

case class Strlen(keyBuf: Buf) extends StrictKeyCommand {
  val command: String = Commands.STRLEN
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.STRLEN, keyBuf))
}

trait MultiSet extends KeysCommand {
  val kv: Map[Buf, Buf]
  override lazy val keys: Seq[ChannelBuffer] =
    kv.keys.map(BufChannelBuffer(_)).toSeq
}