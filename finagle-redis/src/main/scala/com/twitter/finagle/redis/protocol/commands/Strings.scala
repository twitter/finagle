package com.twitter.finagle.redis
package protocol

import util._
import Commands.trimList

case class Append(key: String, value: Array[Byte])
  extends StrictKeyCommand
  with StrictValueCommand
{
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(Commands.APPEND),
    StringToBytes(key),
    value))
}
object Append {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2)
    new Append(BytesToString(list(0)), list(1))
  }
}

case class Decr(override val key: String) extends DecrBy(key, 1) {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.DECR, key))
}
object Decr {
  def apply(args: List[Array[Byte]]) = {
    new Decr(BytesToString(trimList(args, 1, "DECR")(0)))
  }
}
class DecrBy(val key: String, val amount: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.DECRBY, key, amount.toString))
  override def toString = "DecrBy(%s, %d)".format(key, amount)
  override def equals(other: Any) = other match {
    case that: DecrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any) = other.isInstanceOf[DecrBy]
}
object DecrBy {
  def apply(key: String, amount: Int) = new DecrBy(key, amount)
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 2, "DECRBY"))
    val amount = RequireClientProtocol.safe {
      NumberFormat.toInt(list(1))
    }
    new DecrBy(list(0), amount)
  }
}

case class Get(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.GET, key))
}
object Get {
  def apply(args: List[Array[Byte]]) = {
    new Get(BytesToString(trimList(args, 1, "GET")(0)))
  }
}

case class GetBit(key: String, offset: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.GETBIT, key, offset.toString))
}
object GetBit {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,2,"GETBIT"))
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    new GetBit(list(0), offset)
  }
}

case class GetRange(key: String, start: Int, end: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.GETRANGE, key, start.toString, end.toString))
}
object GetRange {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,3,"GETRANGE"))
    val start = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val end = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new GetRange(list(0), start, end)
  }
}

case class GetSet(key: String, value: Array[Byte])
  extends StrictKeyCommand
  with StrictValueCommand
{
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(Commands.GETSET),
    StringToBytes(key),
    value))
}
object GetSet {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "GETSET")
    new GetSet(BytesToString(list(0)), list(1))
  }
}

case class Incr(override val key: String) extends IncrBy(key, 1) {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.INCR, key))
}
object Incr {
  def apply(args: List[Array[Byte]]) = {
    new Incr(BytesToString(trimList(args, 1, "INCR")(0)))
  }
}

class IncrBy(val key: String, val amount: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.INCRBY, key, amount.toString))
  override def toString = "IncrBy(%s, %d)".format(key, amount)
  override def equals(other: Any) = other match {
    case that: IncrBy => that.canEqual(this) && this.key == that.key && this.amount == that.amount
    case _ => false
  }
  def canEqual(other: Any) = other.isInstanceOf[IncrBy]
}
object IncrBy {
  def apply(key: String, amount: Int) = new IncrBy(key, amount)
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 2, "INCRBY"))
    val amount = RequireClientProtocol.safe {
      NumberFormat.toInt(list(1))
    }
    new IncrBy(list(0), amount)
  }
}

case class MGet(keys: List[String]) extends StrictKeysCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(Commands.MGET +: keys)
}

case class MSet(kv: Map[String, Array[Byte]]) extends MultiSet {
  validate()
  val command = MSet.command
}
object MSet extends MultiSetCompanion {
  val command = Commands.MSET
  def get(map: Map[String, Array[Byte]]) = new MSet(map)
}

case class MSetNx(kv: Map[String, Array[Byte]]) extends MultiSet {
  validate()
  val command = MSetNx.command
}
object MSetNx extends MultiSetCompanion {
  val command = Commands.MSETNX
  def get(map: Map[String, Array[Byte]]) = new MSetNx(map)
}

case class Set(key: String, value: Array[Byte])
  extends StrictKeyCommand
  with SetCommand
  with StrictValueCommand
{
  val command = Set.command
}
object Set extends SetCommandCompanion {
  val command = Commands.SET
  def get(key: String, value: Array[Byte]) = new Set(key, value)
}

case class SetBit(key: String, offset: Int, value: Int) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.SETBIT, key, offset.toString, value.toString))
}
object SetBit {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,3,"SETBIT"))
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val value = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new SetBit(list(0), offset, value)
  }
}

case class SetEx(key: String, seconds: Long, value: Array[Byte])
  extends StrictKeyCommand
  with StrictValueCommand
{
  RequireClientProtocol(seconds > 0, "Seconds must be greater than 0")
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(Commands.SETEX),
    StringToBytes(key),
    StringToBytes(seconds.toString),
    value
    ))
}
object SetEx {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 3, "SETEX")
    val seconds = RequireClientProtocol.safe { NumberFormat.toLong(BytesToString(list(1))) }
    new SetEx(BytesToString(list(0)), seconds, list(2))
  }
}

case class SetNx(key: String, value: Array[Byte])
  extends StrictKeyCommand
  with SetCommand
  with StrictValueCommand
{
  val command = SetNx.command
}
object SetNx extends SetCommandCompanion {
  val command = Commands.SETNX
  def get(key: String, value: Array[Byte]) = new SetNx(key, value)
}

case class SetRange(key: String, offset: Int, value: Array[Byte])
  extends StrictKeyCommand
  with StrictValueCommand
{
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(Commands.SETRANGE),
    StringToBytes(key),
    StringToBytes(offset.toString),
    value
  ))
}
object SetRange {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args,3,"SETRANGE")
    val key = BytesToString(list(0))
    val offset = RequireClientProtocol.safe { NumberFormat.toInt(BytesToString(list(1))) }
    val value = list(2)
    new SetRange(key, offset, value)
  }
}

case class Strlen(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.STRLEN, key))
}
object Strlen {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args,1,"STRLEN"))
    new Strlen(list(0))
  }
}

/** Helpers for common idioms */
trait SetCommand extends KeyCommand with ValueCommand {
  val command: String

  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(command),
    StringToBytes(key),
    value
  ))
}
trait SetCommandCompanion {
  val command: String
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, command)
    get(BytesToString(list(0)), list(1))
  }
  def get(key: String, value: Array[Byte]): SetCommand
}

trait MultiSet extends KeysCommand {
  val kv: Map[String, Array[Byte]]
  val command: String
  override lazy val keys: List[String] = kv.keys.toList

  override def toChannelBuffer = {
    val kvList: List[Array[Byte]] = kv.keys.zip(kv.values).flatMap { case(k,v) =>
      StringToBytes(k) :: v :: Nil
    }(collection.breakOut)
    RedisCodec.toUnifiedFormat(StringToBytes(command) :: kvList)
  }
}
trait MultiSetCompanion {
  val command: String
  def apply(args: List[Array[Byte]]) = {
    val length = args.length

    RequireClientProtocol(
      length % 2 == 0 && length > 0,
      "Expected even number of k/v pairs for " + command)

    val map = args.grouped(2).map {
      case key :: value :: Nil => (BytesToString(key), value)
      case _ => throw new ClientError("Unexpected uneven pair of elements in MSET")
    }.toMap
    RequireClientProtocol(map.size == length/2, "Broken mapping, map size not equal to group size")
    get(map)
  }
  def get(map: Map[String, Array[Byte]]): MultiSet
}
