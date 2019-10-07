package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class Append(key: Buf, value: Buf) extends StrictKeyCommand with StrictValueCommand {

  def name: Buf = Command.APPEND
  override def body: Seq[Buf] = Seq(key, value)
}

case class BitCount(key: Buf, start: Option[Int] = None, end: Option[Int] = None)
    extends StrictKeyCommand {

  RequireClientProtocol(
    start.isEmpty && end.isEmpty ||
      start.isDefined && end.isDefined,
    "Both start and end must be specified"
  )

  def name: Buf = Command.BITCOUNT
  override def body: Seq[Buf] = {
    Seq(key) ++
      (start match {
        case Some(i) => Seq(Buf.Utf8(i.toString))
        case None => Seq.empty
      }) ++ (end match {
      case Some(i) => Seq(Buf.Utf8(i.toString))
      case None => Seq.empty
    })
  }
}

case class BitOp(op: Buf, dstKey: Buf, srcKeys: Seq[Buf]) extends Command {
  RequireClientProtocol(
    (op equals BitOp.And) || (op equals BitOp.Or) ||
      (op equals BitOp.Xor) || (op equals BitOp.Not),
    "BITOP supports only AND/OR/XOR/NOT"
  )
  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")
  RequireClientProtocol(
    !op.equals(BitOp.Not) || srcKeys.size == 1,
    "NOT operation takes only 1 input key"
  )

  def name: Buf = Command.BITOP
  override def body: Seq[Buf] = Seq(op, dstKey) ++ srcKeys
}

object BitOp {
  val And = Buf.Utf8("AND")
  val Or = Buf.Utf8("OR")
  val Xor = Buf.Utf8("XOR")
  val Not = Buf.Utf8("NOT")
}

case class Decr(override val key: Buf) extends DecrBy(key, 1) {
  override def name: Buf = Command.DECR
  override def body: Seq[Buf] = Seq(key)
}

class DecrBy(val key: Buf, val amount: Long) extends StrictKeyCommand {
  def name: Buf = Command.DECRBY
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(amount.toString))

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
  def name: Buf = Command.GET
}

case class GetBit(key: Buf, offset: Int) extends StrictKeyCommand {
  def name: Buf = Command.GETBIT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(offset.toString))
}

case class GetRange(key: Buf, start: Long, end: Long) extends StrictKeyCommand {
  def name: Buf = Command.GETRANGE
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(start.toString), Buf.Utf8(end.toString))
}

case class GetSet(key: Buf, value: Buf) extends StrictKeyCommand with StrictValueCommand {

  def name: Buf = Command.GETSET
  override def body: Seq[Buf] = Seq(key, value)
}

case class Incr(override val key: Buf) extends IncrBy(key, 1) {
  override def name: Buf = Command.INCR
  override def body: Seq[Buf] = Seq(key)
}

class IncrBy(val key: Buf, val amount: Long) extends StrictKeyCommand {
  def name: Buf = Command.INCRBY
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(amount.toString))
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
  def name: Buf = Command.MGET
  override def body: Seq[Buf] = keys
}

case class MSet(kv: Map[Buf, Buf]) extends MultiSet {
  validate()

  def name: Buf = Command.MSET
}

case class MSetNx(kv: Map[Buf, Buf]) extends MultiSet {
  validate()

  def name: Buf = Command.MSETNX
}

case class PSetEx(key: Buf, millis: Long, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {

  RequireClientProtocol(millis > 0, "Milliseconds must be greater than 0")

  def name: Buf = Command.PSETEX
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(millis.toString), value)
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

  def name: Buf = Command.SET
  override def body: Seq[Buf] = {
    val keyAndValue = Seq(key, value)

    val maybeTtl = ttl match {
      case Some(InSeconds(seconds)) =>
        Seq(Set.ExBytes, Buf.Utf8(seconds.toString))
      case Some(InMilliseconds(millis)) =>
        Seq(Set.PxBytes, Buf.Utf8(millis.toString))
      case _ => Nil
    }

    val nxAndXx = (if (nx) Seq(Set.NxBytes) else Nil) ++ (if (xx) Seq(Set.XxBytes) else Nil)

    keyAndValue ++ maybeTtl ++ nxAndXx
  }
}
object Set {
  private val Ex = "EX"
  private val Px = "PX"
  private val Nx = "NX"
  private val Xx = "XX"

  private val ExBytes = Buf.Utf8(Ex)
  private val PxBytes = Buf.Utf8(Px)
  private val NxBytes = Buf.Utf8(Nx)
  private val XxBytes = Buf.Utf8(Xx)
}

case class SetBit(key: Buf, offset: Int, value: Int) extends StrictKeyCommand {
  def name: Buf = Command.SETBIT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(offset.toString), Buf.Utf8(value.toString))
}

case class SetEx(key: Buf, seconds: Long, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")

  def name: Buf = Command.SETEX
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(seconds.toString), value)
}

case class SetNx(key: Buf, value: Buf) extends StrictKeyCommand with StrictValueCommand {

  def name: Buf = Command.SETNX
  override def body: Seq[Buf] = Seq(key, value)
}

case class SetRange(key: Buf, offset: Int, value: Buf)
    extends StrictKeyCommand
    with StrictValueCommand {

  def name: Buf = Command.SETRANGE
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(offset.toString), value)
}

case class Strlen(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.STRLEN
}

trait MultiSet extends KeysCommand {
  def kv: Map[Buf, Buf]
  def keys: Seq[Buf] = kv.keys.toSeq
  override def body: Seq[Buf] =
    kv.iterator.flatMap { case (k, v) => k :: v :: Nil }.toSeq
}
