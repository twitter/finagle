package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
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
object Append {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "APPEND")
    new Append(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
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
object BitCount {
  def apply(args: Seq[Array[Byte]]) = {
    if (args != null && args.size == 1) {
      new BitCount(Buf.ByteArray.Owned(args(0)))
    } else {
      val list = trimList(args, 3, "BITCOUNT")
      val start = RequireClientProtocol.safe {
        NumberFormat.toInt(BytesToString(list(1)))
      }
      val end = RequireClientProtocol.safe {
        NumberFormat.toInt(BytesToString(list(2)))
      }
      new BitCount(Buf.ByteArray.Owned(list(0)),
        Some(start), Some(end))
    }
  }
}

case class BitOp(op: Buf, dstKey: Buf,
    srcKeys: Seq[Buf]) extends Command {
  val command: String = Commands.BITOP
  RequireClientProtocol((op equals BitOp.And) || (op equals BitOp.Or) ||
    (op equals BitOp.Xor) || (op equals BitOp.Not),
    "BITOP supports only AND/OR/XOR/NOT")
  RequireClientProtocol(srcKeys.size > 0, "srcKeys must not be empty")
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

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.size >= 3,
      "BITOP expected at least 3 elements, found %d".format(args.size))
    val list = args.map(Buf.ByteArray.Owned(_))
    if (list(0) equals Not) {
      RequireClientProtocol(args.size == 3,
        "BITOP expected 3 elements when op is NOT, found %d".format(args.size))
      new BitOp(list(0), list(1), Seq(list(2)))
    } else {
      new BitOp(list(0), list(1), list.drop(2))
    }
  }
}

case class Decr(override val keyBuf: Buf) extends DecrBy(keyBuf, 1) {
  override val command: String = Commands.DECR
  override def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.DECR, keyBuf))
}
object Decr {
  def apply(args: Seq[Array[Byte]]) = {
    new Decr(Buf.ByteArray.Owned(trimList(args, 1, "DECR")(0)))
  }
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
  def apply(args: Seq[Array[Byte]]) = {
    val amount = RequireClientProtocol.safe {
      NumberFormat.toLong(BytesToString(args(1)))
    }
    new DecrBy(Buf.ByteArray.Owned(args(0)), amount)
  }
  def apply(keyBuf: Buf, amount: Long) = {
    new DecrBy(keyBuf, amount)
  }
}

case class Get(keyBuf: Buf) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.GET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.GET, keyBuf))
}
object Get {
  def apply(args: Seq[Array[Byte]]) = {
    new Get(Buf.ByteArray.Owned(trimList(args, 1, "GET")(0)))
  }
}

case class GetBit(keyBuf: Buf, offset: Int) extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  val command: String = Commands.GETBIT
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.GETBIT,
    keyBuf, StringToBuf(offset.toString)))
}
object GetBit {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,2,"GETBIT")
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(BytesToString(list(1))) }
    new GetBit(Buf.ByteArray.Owned(list(0)), offset)
  }
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
object GetRange {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,3,"GETRANGE")
    val start = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(1))) }
    val end = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(2))) }
    new GetRange(Buf.ByteArray.Owned(list(0)), start, end)
  }
}

case class GetSet(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.GETSET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.GETSET, keyBuf, valueBuf))
}
object GetSet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "GETSET")
    new GetSet(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}

case class Incr(override val keyBuf: Buf) extends IncrBy(keyBuf, 1) {
  override val command: String = Commands.INCR
  override def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.INCR, keyBuf))
}
object Incr {
  def apply(args: Seq[Array[Byte]]) = {
    new Incr(Buf.ByteArray.Owned(trimList(args, 1, "INCR")(0)))
  }
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
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 2, "INCRBY"))
    val amount = RequireClientProtocol.safe {
      NumberFormat.toLong(list(1))
    }
    new IncrBy(Buf.ByteArray.Owned(args(0)), amount)
  }
}

case class MGet(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer(_))
  val command: String = Commands.MGET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.MGET +: keysBuf)
}

object MGet {
  def apply(args: => Seq[Array[Byte]]) =
    new MGet(args.map(Buf.ByteArray.Owned(_)))
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
object MSet extends MultiSetCompanion {
  val command: String = Commands.MSET
  def get(map: Map[Buf, Buf]) = new MSet(map)
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
object MSetNx extends MultiSetCompanion {
  def get(map: Map[Buf, Buf]) = new MSetNx(map)
}

case class PSetEx(keyBuf: Buf, millis: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
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
object PSetEx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "PSETEX")
    val millis = RequireClientProtocol.safe {
      NumberFormat.toLong(BytesToString(list(1)))
    }
    new PSetEx(Buf.ByteArray.Owned(args(0)), millis,
      Buf.ByteArray.Owned(list(2)))
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

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.size >= 2, "SET requires at least 2 arguments")

    val key = Buf.ByteArray.Owned(args(0))
    val value = Buf.ByteArray.Owned(args(1))

    val set = new Set(Buf.ByteArray.Owned(args(0)),
      Buf.ByteArray.Owned(args(1)))

    def run(args: Seq[Array[Byte]], set: Set): Set = {
      args.headOption match {
        case None => set
        case Some(bytes) => {
          val flag = new String(bytes, "UTF-8").toUpperCase
          flag match {
            case Ex => args.tail.headOption match {
              case None => throw ClientError("Invalid syntax for SET")
              case Some(bytes) => run(args.tail.tail,
                set.copy(ttl = Some(InSeconds(RequireClientProtocol.safe {
                  NumberFormat.toLong(BytesToString(bytes))
                }))))
            }
            case Px => args.tail.headOption match {
              case None => throw ClientError("Invalid syntax for SET")
              case Some(bytes) => run(args.tail.tail,
                set.copy(ttl = Some(InMilliseconds(RequireClientProtocol.safe {
                  NumberFormat.toLong(BytesToString(bytes))
                }))))
            }
            case Nx => run(args.tail, set.copy(nx = true))
            case Xx => run(args.tail, set.copy(xx = true))
            case _ => throw ClientError("Invalid syntax for SET")
          }
        }
      }
    }

    run(args.drop(2), set)
  }
}

case class SetBit(keyBuf: Buf, offset: Int, value: Int) extends StrictKeyCommand {
  val command: String = Commands.SETBIT
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def toChannelBuffer: ChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SETBIT, keyBuf,
      StringToBuf(offset.toString),
      StringToBuf(value.toString)))
}
object SetBit {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,3,"SETBIT"))
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val value = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new SetBit(Buf.ByteArray.Owned(args(0)), offset, value)
  }
}

case class SetEx(keyBuf: Buf, seconds: Long, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
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
object SetEx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "SETEX")
    val seconds = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(1))) }
    new SetEx(
      Buf.ByteArray.Owned(args(0)),
      seconds,
      Buf.ByteArray.Owned(list(2))
    )
  }
}

case class SetNx(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  val command: String = Commands.SETNX
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SETNX, keyBuf, valueBuf))
}
object SetNx {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length > 1, "SETNX requires at least one member")
    new SetNx(Buf.ByteArray.Owned(args(0)), Buf.ByteArray.Owned(args(1)))
  }
}

case class SetRange(keyBuf: Buf, offset: Int, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand
{
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
object SetRange {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,3,"SETRANGE")
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(BytesToString(list(1))) }
    val value = list(2)
    new SetRange(
      Buf.ByteArray.Owned(list(0)),
      offset,
      Buf.ByteArray.Owned(value)
    )
  }
}

case class Strlen(keyBuf: Buf) extends StrictKeyCommand {
  val command: String = Commands.STRLEN
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.STRLEN, keyBuf))
}
object Strlen {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "STRLEN requires at least one member")
    new Strlen(Buf.ByteArray.Owned(args(0)))
  }
}

trait MultiSet extends KeysCommand {
  val kv: Map[Buf, Buf]
  override lazy val keys: Seq[ChannelBuffer] =
    kv.keys.map(BufChannelBuffer(_)).toSeq
}
trait MultiSetCompanion {
  def apply(args: Seq[Array[Byte]]) = {
    val length = args.length

    RequireClientProtocol(
      length % 2 == 0 && length > 0,
      "Expected even number of k/v pairs")

    val map = args.grouped(2).map {
      case key :: value :: Nil => (Buf.ByteArray.Owned(key),
        Buf.ByteArray.Owned(value))
      case _ => throw ClientError("Unexpected uneven pair of elements in MSET")
    }.toMap
    RequireClientProtocol(map.size == length/2, "Broken mapping, map size not equal to group size")
    get(map)
  }
  def get(map: Map[Buf, Buf]): MultiSet
}
