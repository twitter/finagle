package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Append(key: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.APPEND
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.APPEND, key, value))
}
object Append {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "APPEND")
    new Append(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class BitCount(key: ChannelBuffer, start: Option[Int] = None,
    end: Option[Int] = None) extends StrictKeyCommand {
  val command = Commands.BITCOUNT
  RequireClientProtocol(start.isEmpty && end.isEmpty ||
    start.isDefined && end.isDefined, "Both start and end must be specified")
  def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.BITCOUNT, key) ++
      (start match {
        case Some(i) => Seq(StringToChannelBuffer(i.toString))
        case None => Seq.empty
      }) ++ (end match {
        case Some(i) => Seq(StringToChannelBuffer(i.toString))
        case None => Seq.empty
      }))
  }
}
object BitCount {
  def apply(args: Seq[Array[Byte]]) = {
    if (args != null && args.size == 1) {
      new BitCount(ChannelBuffers.wrappedBuffer(args(0)))
    } else {
      val list = trimList(args, 3, "BITCOUNT")
      val start = RequireClientProtocol.safe {
        NumberFormat.toInt(BytesToString(list(1)))
      }
      val end = RequireClientProtocol.safe {
        NumberFormat.toInt(BytesToString(list(2)))
      }
      new BitCount(ChannelBuffers.wrappedBuffer(list(0)),
        Some(start), Some(end))
    }
  }
}

case class BitOp(op: ChannelBuffer, dstKey: ChannelBuffer,
    srcKeys: Seq[ChannelBuffer]) extends Command {
  val command = Commands.BITOP
  RequireClientProtocol((op equals BitOp.And) || (op equals BitOp.Or) ||
    (op equals BitOp.Xor) || (op equals BitOp.Not),
    "BITOP supports only AND/OR/XOR/NOT")
  RequireClientProtocol(srcKeys.size > 0, "srcKeys must not be empty")
  RequireClientProtocol(!op.equals(BitOp.Not) || srcKeys.size == 1,
    "NOT operation takes only 1 input key")
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.BITOP, op, dstKey) ++ srcKeys)
}

object BitOp {
  val And = StringToChannelBuffer("AND")
  val Or = StringToChannelBuffer("OR")
  val Xor = StringToChannelBuffer("XOR")
  val Not = StringToChannelBuffer("NOT")

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.size >= 3,
      "BITOP expected at least 3 elements, found %d".format(args.size))
    val list = args map (ChannelBuffers.wrappedBuffer)
    if (list(0) equals Not) {
      RequireClientProtocol(args.size == 3,
        "BITOP expected 3 elements when op is NOT, found %d".format(args.size))
      new BitOp(list(0), list(1), Seq(list(2)))
    } else {
      new BitOp(list(0), list(1), list.drop(2))
    }
  }
}

case class Decr(override val key: ChannelBuffer) extends DecrBy(key, 1) {
  override val command = Commands.DECR
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.DECR, key))
}
object Decr {
  def apply(args: Seq[Array[Byte]]) = {
    new Decr(ChannelBuffers.wrappedBuffer(trimList(args, 1, "DECR")(0)))
  }
}
class DecrBy(val key: ChannelBuffer, val amount: Long) extends StrictKeyCommand {
  val command = Commands.DECRBY
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
      CommandBytes.DECRBY,
      key,
      StringToChannelBuffer(amount.toString)))
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
    new DecrBy(ChannelBuffers.wrappedBuffer(args(0)), amount)
  }
  def apply(key: ChannelBuffer, amount: Long) = {
    new DecrBy(key, amount)
  }
}

case class Get(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.GET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.GET, key))
}
object Get {
  def apply(args: Seq[Array[Byte]]) = {
    new Get(ChannelBuffers.wrappedBuffer(trimList(args, 1, "GET")(0)))
  }
}

case class GetBit(key: ChannelBuffer, offset: Int) extends StrictKeyCommand {
  val command = Commands.GETBIT
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.GETBIT,
    key, StringToChannelBuffer(offset.toString)))
}
object GetBit {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,2,"GETBIT")
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(BytesToString(list(1))) }
    new GetBit(ChannelBuffers.wrappedBuffer(list(0)), offset)
  }
}

case class GetRange(key: ChannelBuffer, start: Long, end: Long) extends StrictKeyCommand {
  val command = Commands.GETRANGE
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.GETRANGE, key,
      StringToChannelBuffer(start.toString),
      StringToChannelBuffer(end.toString)
    ))
}
object GetRange {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,3,"GETRANGE")
    val start = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(1))) }
    val end = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(2))) }
    new GetRange(ChannelBuffers.wrappedBuffer(list(0)), start, end)
  }
}

case class GetSet(key: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.GETSET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.GETSET, key, value))
}
object GetSet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "GETSET")
    new GetSet(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class Incr(override val key: ChannelBuffer) extends IncrBy(key, 1) {
  override val command = Commands.INCR
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.INCR, key))
}
object Incr {
  def apply(args: Seq[Array[Byte]]) = {
    new Incr(ChannelBuffers.wrappedBuffer(trimList(args, 1, "INCR")(0)))
  }
}

class IncrBy(val key: ChannelBuffer, val amount: Long) extends StrictKeyCommand {
  val command = Commands.INCRBY
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.INCRBY, key,
      StringToChannelBuffer(amount.toString)))
  override def toString = "IncrBy(%s, %d)".format(key, amount)
  override def equals(other: Any) = other match {
    case that: IncrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any) = other.isInstanceOf[IncrBy]
}
object IncrBy {
  def apply(key: ChannelBuffer, amount: Long) = new IncrBy(key, amount)
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 2, "INCRBY"))
    val amount = RequireClientProtocol.safe {
      NumberFormat.toLong(list(1))
    }
    new IncrBy(ChannelBuffers.wrappedBuffer(args(0)), amount)
  }
}

case class MGet(keys: Seq[ChannelBuffer]) extends StrictKeysCommand {
  val command = Commands.MGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.MGET +: keys)
}
object MGet {
  def apply(args: => Seq[Array[Byte]]) =
    new MGet(args.map(ChannelBuffers.wrappedBuffer(_)))
}

case class MSet(kv: Map[ChannelBuffer, ChannelBuffer]) extends MultiSet {
  validate()
  val command = Commands.MSET

  def toChannelBuffer = {
    val kvList: Seq[ChannelBuffer] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.toUnifiedFormat(CommandBytes.MSET +: kvList)
  }
}
object MSet extends MultiSetCompanion {
  val command = Commands.MSET
  def get(map: Map[ChannelBuffer, ChannelBuffer]) = new MSet(map)
}

case class MSetNx(kv: Map[ChannelBuffer, ChannelBuffer]) extends MultiSet {
  validate()
  val command = Commands.MSETNX

  def toChannelBuffer = {
    val kvList: Seq[ChannelBuffer] = kv.flatMap { case(k,v) =>
      k :: v :: Nil
    }(collection.breakOut)
    RedisCodec.toUnifiedFormat(CommandBytes.MSETNX +: kvList)
  }
}
object MSetNx extends MultiSetCompanion {
  def get(map: Map[ChannelBuffer, ChannelBuffer]) = new MSetNx(map)
}

case class PSetEx(key: ChannelBuffer, millis: Long, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.PSETEX
  RequireClientProtocol(millis > 0, "Milliseconds must be greater than 0")
  def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(Seq(
      CommandBytes.PSETEX,
      key,
      StringToChannelBuffer(millis.toString),
      value))
  }
}
object PSetEx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "PSETEX")
    val millis = RequireClientProtocol.safe {
      NumberFormat.toLong(BytesToString(list(1)))
    }
    new PSetEx(ChannelBuffers.wrappedBuffer(args(0)), millis,
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}

sealed trait TimeToLive
case class InSeconds(seconds: Long) extends TimeToLive
case class InMilliseconds(millis: Long) extends TimeToLive

case class Set(
  key: ChannelBuffer,
  value: ChannelBuffer,
  ttl: Option[TimeToLive] = None,
  nx: Boolean = false,
  xx: Boolean = false)
    extends StrictKeyCommand
    with StrictValueCommand {
  val command = Commands.SET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
    Seq(CommandBytes.SET, key, value) ++
      (ttl match {
        case Some(InSeconds(seconds)) =>
          Seq(Set.ExBytes, StringToChannelBuffer(seconds.toString))
        case Some(InMilliseconds(millis)) =>
          Seq(Set.PxBytes, StringToChannelBuffer(millis.toString))
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

  private val ExBytes = StringToChannelBuffer(Ex)
  private val PxBytes = StringToChannelBuffer(Px)
  private val NxBytes = StringToChannelBuffer(Nx)
  private val XxBytes = StringToChannelBuffer(Xx)

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.size >= 2, "SET requires at least 2 arguments")

    val key = ChannelBuffers.wrappedBuffer(args(0))
    val value = ChannelBuffers.wrappedBuffer(args(1))

    val set = new Set(ChannelBuffers.wrappedBuffer(args(0)),
      ChannelBuffers.wrappedBuffer(args(1)))

    def run(args: Seq[Array[Byte]], set: Set): Set = {
      args.headOption match {
        case None => set
        case Some(bytes) => {
          val flag = CBToString(ChannelBuffers.wrappedBuffer(bytes)).toUpperCase
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

case class SetBit(key: ChannelBuffer, offset: Int, value: Int) extends StrictKeyCommand {
  val command = Commands.SETBIT
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SETBIT, key,
      StringToChannelBuffer(offset.toString),
      StringToChannelBuffer(value.toString)))
}
object SetBit {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,3,"SETBIT"))
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val value = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new SetBit(ChannelBuffers.wrappedBuffer(args(0)), offset, value)
  }
}

case class SetEx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.SETEX
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.SETEX,
    key,
    StringToChannelBuffer(seconds.toString),
    value
  ))
}
object SetEx {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "SETEX")
    val seconds = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(1))) }
    new SetEx(
      ChannelBuffers.wrappedBuffer(args(0)),
      seconds,
      ChannelBuffers.wrappedBuffer(list(2))
    )
  }
}

case class SetNx(key: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.SETNX
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.SETNX, key, value))
}
object SetNx {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length > 1, "SETNX requires at least one member")
    new SetNx(ChannelBuffers.wrappedBuffer(args(0)), ChannelBuffers.wrappedBuffer(args(1)))
  }
}

case class SetRange(key: ChannelBuffer, offset: Int, value: ChannelBuffer)
  extends StrictKeyCommand
  with StrictValueCommand
{
  val command = Commands.SETRANGE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.SETRANGE,
    key,
    StringToChannelBuffer(offset.toString),
    value
  ))
}
object SetRange {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args,3,"SETRANGE")
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(BytesToString(list(1))) }
    val value = list(2)
    new SetRange(
      ChannelBuffers.wrappedBuffer(list(0)),
      offset,
      ChannelBuffers.wrappedBuffer(value)
    )
  }
}

case class Strlen(key: ChannelBuffer) extends StrictKeyCommand {
  val command = Commands.STRLEN
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.STRLEN, key))
}
object Strlen {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "STRLEN requires at least one member")
    new Strlen(ChannelBuffers.wrappedBuffer(args(0)))
  }
}

trait MultiSet extends KeysCommand {
  val kv: Map[ChannelBuffer, ChannelBuffer]
  override lazy val keys: Seq[ChannelBuffer] = kv.keys.toList
}
trait MultiSetCompanion {
  def apply(args: Seq[Array[Byte]]) = {
    val length = args.length

    RequireClientProtocol(
      length % 2 == 0 && length > 0,
      "Expected even number of k/v pairs")

    val map = args.grouped(2).map {
      case key :: value :: Nil => (ChannelBuffers.wrappedBuffer(key),
        ChannelBuffers.wrappedBuffer(value))
      case _ => throw ClientError("Unexpected uneven pair of elements in MSET")
    }.toMap
    RequireClientProtocol(map.size == length/2, "Broken mapping, map size not equal to group size")
    get(map)
  }
  def get(map: Map[ChannelBuffer, ChannelBuffer]): MultiSet
}
