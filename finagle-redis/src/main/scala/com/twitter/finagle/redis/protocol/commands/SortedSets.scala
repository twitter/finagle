package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class ZAdd(keyBuf: Buf, members: Seq[ZMember])
  extends StrictKeyCommand
  with StrictZMembersCommand {

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZADD
  def toChannelBuffer = {
    val cmds = Seq(CommandBytes.ZADD, keyBuf)

    RedisCodec.bufToUnifiedChannelBuffer(cmds ++ membersBufs)
  }
}
object ZAdd {
  def apply(args: Seq[Array[Byte]]) = args match {
    case head :: tail =>
      new ZAdd(Buf.ByteArray.Owned(head), ZMembers(tail))
    case _ =>
      throw ClientError("Invalid use of ZADD")
  }
}

case class ZCard(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZCARD
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.ZCARD, keyBuf))
}
object ZCard {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "ZCARD requires at least one member")
    new ZCard(Buf.ByteArray.Owned(args(0)))
  }
}

case class ZCount(keyBuf: Buf, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZCOUNT
  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(
      CommandBytes.ZCOUNT,
      keyBuf,
      Buf.Utf8(min.toString),
      Buf.Utf8(max.toString)
    ))
}
object ZCount {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZCOUNT"))
    new ZCount(Buf.ByteArray.Owned(args(0)), ZInterval(list(1)), ZInterval(list(2)))
  }
}

case class ZIncrBy(keyBuf: Buf, amount: Double, memberBuf: Buf)
  extends StrictKeyCommand
  with StrictMemberCommand {

  override def member: ChannelBuffer = BufChannelBuffer(memberBuf)
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZINCRBY
  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(
      CommandBytes.ZINCRBY,
      keyBuf,
      Buf.Utf8(amount.toString),
      memberBuf))
}
object ZIncrBy {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "ZINCRBY")
    val key = list(0)
    val amount = RequireClientProtocol.safe {
      NumberFormat.toDouble(BytesToString(list(1)))
    }
    new ZIncrBy(
      Buf.ByteArray.Owned(key),
      amount,
      Buf.ByteArray.Owned(list(2)))
  }
}

case class ZInterStore(
    destination: Buf,
    numkeys: Int,
    keysBuf: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore
{
  def command = Commands.ZINTERSTORE
  def commandBytes = CommandBytes.ZINTERSTORE
  validate()
}

object ZInterStore extends ZStoreCompanion {
  def get(
    dest: String,
    numkeys: Int,
    keys: Seq[String],
    weights: Option[Weights],
    agg: Option[Aggregate]) =
      new ZInterStore(
        Buf.Utf8(dest),
        numkeys,
        keys.map(Buf.Utf8.apply),
        weights,
        agg)
}

case class ZRange(keyBuf: Buf, start: Long, stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  def command = Commands.ZRANGE
  def commandBytes = CommandBytes.ZRANGE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)
}
object ZRange extends ZRangeCmdCompanion {
  override def get(key: Buf, start: Long, stop: Long, withScores: Option[CommandArgument]) =
    new ZRange(key, start, stop, withScores)
}

case class ZRangeByScore(
    keyBuf: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange
{
  def command = Commands.ZRANGEBYSCORE
  def commandBytes = CommandBytes.ZRANGEBYSCORE
  validate()

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)

  private[redis] def encoded: Seq[Buf] = {
    def command = Seq(keyBuf, Buf.Utf8(min.value), Buf.Utf8(max.value))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }
}

object ZRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange =
      new ZRangeByScore(key, min, max, withScores, limit)
}

case class ZRank(keyBuf: Buf, memberBuf: Buf) extends ZRankCmd {
  def command = Commands.ZRANK
  def commandBytes = CommandBytes.ZRANK
}
object ZRank extends ZRankCmdCompanion {
  def get(key: Buf, member: Buf) =
    new ZRank(key, member)
}

case class ZRem(keyBuf: Buf, members: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(
    members != null && members.length > 0,
    "Members list must not be empty for ZREM")

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZREM
  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.ZREM, keyBuf) ++ members)
}
object ZRem {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.length > 1, "ZREM requires at least one member")
    val key = Buf.ByteArray.Owned(args(0))
    val remaining = args.drop(1).map(Buf.ByteArray.Owned.apply)
    new ZRem(key, remaining)
  }
}

case class ZRemRangeByRank(keyBuf: Buf, start: Long, stop: Long) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZREMRANGEBYRANK
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(
    Seq(CommandBytes.ZREMRANGEBYRANK, keyBuf) ++ Seq(
      Buf.Utf8(start.toString),
      Buf.Utf8(stop.toString)
    )
  )
}
object ZRemRangeByRank {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYRANK requires 3 arguments"))
    val key = Buf.ByteArray.Owned(args(0))
    val start = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val stop = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new ZRemRangeByRank(key, start, stop)
  }
}

case class ZRemRangeByScore(keyBuf: Buf, min: ZInterval, max: ZInterval)
  extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZREMRANGEBYSCORE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(
    Seq(CommandBytes.ZREMRANGEBYSCORE, keyBuf) ++ Seq(
      Buf.Utf8(min.toString),
      Buf.Utf8(max.toString)
    )
  )
}
object ZRemRangeByScore {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYSCORE requires 3 arguments"))
    val key = Buf.ByteArray.Owned(args(0))
    val min = ZInterval(list(1))
    val max = ZInterval(list(2))
    new ZRemRangeByScore(key, min, max)
  }
}

case class ZRevRange(
    keyBuf: Buf,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  def command = Commands.ZREVRANGE
  def commandBytes = CommandBytes.ZREVRANGE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)
}
object ZRevRange extends ZRangeCmdCompanion {
  override def get(key: Buf, start: Long, stop: Long, withScores: Option[CommandArgument]) =
    new ZRevRange(key, start, stop, withScores)
}

case class ZRevRangeByScore(
    keyBuf: Buf,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange
{
  def command = Commands.ZREVRANGEBYSCORE
  def commandBytes = CommandBytes.ZREVRANGEBYSCORE
  validate()

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)


  override private[redis] def encoded: Seq[Buf] = {
    def command = Seq(keyBuf, Buf.Utf8(max.value), Buf.Utf8(min.value))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }

}
object ZRevRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: Buf,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange =
      new ZRevRangeByScore(key, max, min, withScores, limit)
}

case class ZRevRank(keyBuf: Buf, memberBuf: Buf) extends ZRankCmd {
  def command = Commands.ZREVRANK
  def commandBytes = CommandBytes.ZREVRANK
}
object ZRevRank extends ZRankCmdCompanion {
  def get(key: Buf, member: Buf) =
    new ZRevRank(key, member)
}

case class ZScore(keyBuf: Buf, memberBuf: Buf)
  extends StrictKeyCommand
  with StrictMemberCommand {

  override def member: ChannelBuffer = BufChannelBuffer(memberBuf)
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZSCORE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.ZSCORE,
    keyBuf,
    memberBuf
  ))
}

object ZScore {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "ZSCORE")
    new ZScore(Buf.ByteArray.Owned(args(0)),Buf.ByteArray.Owned(args(1)))
  }
}

case class ZUnionStore(
    destination: Buf,
    numkeys: Int,
    keysBuf: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore
{
  def command = Commands.ZUNIONSTORE
  def commandBytes = CommandBytes.ZUNIONSTORE
  validate()
}
object ZUnionStore extends ZStoreCompanion {
  def get(
    dest: String,
    numkeys: Int,
    keys: Seq[String],
    weights: Option[Weights],
    agg: Option[Aggregate]) =
      new ZUnionStore(Buf.Utf8(dest),
        numkeys,
        keys.map(Buf.Utf8.apply),
        weights,
        agg)
}

/**
 * Helper Objects
 */

case class ZRangeResults(entries: Array[Buf], scores: Array[Double]) {
  def asTuples(): Seq[(Buf, Double)] =
    (entries, scores).zipped.map { (entry, score) => (entry, score) }.toSeq
}
object ZRangeResults {
  def apply(tuples: Seq[(Buf, Buf)]): ZRangeResults = {
    val arrays = tuples.unzip
    val doubles = arrays._2 map { score =>
      NumberFormat.toDouble(BytesToString(Buf.ByteArray.Owned.extract(score)))
    }

    ZRangeResults(arrays._1.toArray, doubles.toArray)
  }
}

/**
 * Represents part of an interval, helpers in companion object
 * See http://redis.io/commands/zrangebyscore for more info on different intervals
 */
case class ZInterval(value: String) {
  import ZInterval._
  private val representation = value.toLowerCase match {
    case N_INF => N_INF
    case P_INF => P_INF
    case double => double.head match {
      case EXCLUSIVE => RequireClientProtocol.safe {
        NumberFormat.toDouble(double.tail)
        double
      }
      case f => RequireClientProtocol.safe {
        NumberFormat.toDouble(value)
        double
      }
    }
  }

  override def toString = representation
}

object ZInterval {
  private val P_INF = "+inf"
  private val N_INF = "-inf"
  private val EXCLUSIVE = '('

  val MAX = new ZInterval(P_INF)
  val MIN = new ZInterval(N_INF)
  def apply(double: Double) = new ZInterval(double.toString)
  def apply(v: Array[Byte]) = new ZInterval(BytesToString(v))
  def exclusive(double: Double) = new ZInterval("%c%s".format(EXCLUSIVE, double.toString))
}

case class ZMember(score: Double, memberBuf: Buf)
  extends StrictScoreCommand
  with StrictMemberCommand {

  def command = "ZMEMBER"

  override def member = BufChannelBuffer(memberBuf)
  override def toChannelBuffer = member
}


sealed trait ZMembersCommand {
  val members: Seq[ZMember]
}
sealed trait StrictZMembersCommand extends ZMembersCommand {
  RequireClientProtocol(!members.isEmpty, "Members set must not be empty")

  members.foreach { member =>
    RequireClientProtocol(member != null, "Empty member found")
  }

  def membersByteArray: Seq[Array[Byte]] = {
    members.map { member =>
      Seq(
        StringToBytes(member.score.toString),
        Buf.ByteArray.Owned.extract(member.memberBuf)
      )
    }.flatten
  }
  def membersBufs: Seq[Buf] = {
    members.map { member =>
      Seq(
        Buf.Utf8(member.score.toString),
        member.memberBuf
      )
    }.flatten
  }
}

object ZMembers {
  def apply(args: Seq[Array[Byte]]): Seq[ZMember] = {
    val size = args.length
    RequireClientProtocol(size % 2 == 0 && size > 0, "Unexpected uneven pair of elements")

    args.grouped(2).map {
      case score :: member :: Nil =>
        ZMember(
          RequireClientProtocol.safe {
            NumberFormat.toDouble(BytesToString(score))
          },
          Buf.ByteArray.Owned(member))
      case _ =>
        throw ClientError("Unexpected uneven pair of elements in members")
    }.toSeq
  }
}

/**
 * Helper Traits
 */

abstract class ZStore extends KeysCommand {
  val destination: Buf
  val numkeys: Int
  val keysBuf: Seq[Buf]
  val weights: Option[Weights]
  val aggregate: Option[Aggregate]
  def commandBytes: Buf

  override def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer.apply)

  override protected def validate() {
    super.validate()
    RequireClientProtocol(destination.length > 0, "destination must not be empty")
    RequireClientProtocol(numkeys > 0, "numkeys must be > 0")
    RequireClientProtocol(keys.size == numkeys, "must supply the same number of keys as numkeys")
    // ensure if weights are specified they are equal to the size of numkeys
    weights match {
      case Some(list) =>
        RequireClientProtocol(
          list.size == numkeys,
          "If WEIGHTS specified, numkeys weights required")
      case None =>
    }
  }

  def toChannelBuffer = {
    var args = Seq(destination, Buf.Utf8(numkeys.toString)) ++ keysBuf
    weights match {
      case Some(wlist) => args = args ++ wlist.encoded
      case None =>
    }
    aggregate match {
      case Some(agg) => args = args ++ agg.encoded
      case None =>
    }
    RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: args)
  }
}
trait ZStoreCompanion {
  def apply(dest: String, keys: Seq[String]) = get(dest, keys.length, keys, None, None)
  def apply(dest: String, keys: Seq[String], weights: Weights) = {
    get(dest, keys.length, keys, Some(weights), None)
  }
  def apply(dest: String, keys: Seq[String], agg: Aggregate) =
    get(dest, keys.length, keys, None, Some(agg))
  def apply(dest: String, keys: Seq[String], weights: Weights, agg: Aggregate) =
    get(dest, keys.length, keys, Some(weights), Some(agg))

  /** get a new instance of the appropriate storage class
   * @param d - Destination
   * @param n - Number of keys
   * @param k - Keys
   * @param w - Weights
   * @param a - Aggregate
   *
   * @return new instance
   */
  def get(d: String, n: Int, k: Seq[String], w: Option[Weights], a: Option[Aggregate]): ZStore

  def apply(args: Seq[Array[Byte]]) = BytesToString.fromList(args) match {
    case destination :: nk :: tail =>
      val numkeys = RequireClientProtocol.safe { NumberFormat.toInt(nk) }
      tail.size match {
        case done if done == numkeys =>
          get(destination, numkeys, tail, None, None)
        case more if more > numkeys =>
          parseArgs(destination, numkeys, tail)
        case _ =>
          throw ClientError("Specified keys must equal numkeys")
      }
    case _ => throw ClientError("Expected a minimum of 3 arguments for command")
  }

  protected def parseArgs(dest: String, numkeys: Int, remaining: Seq[String]) = {
    val (keys, args) = remaining.splitAt(numkeys)
    args.isEmpty match {
      case true =>
        get(dest, numkeys, keys, None, None)
      case false =>
        val (args0, args1) = findArgs(args, numkeys)
        RequireClientProtocol(args0.length > 1, "Length of arguments must be > 1")
        val weights = findWeights(args0, args1)
        val aggregate = findAggregate(args0, args1)
        weights.foreach { w =>
          RequireClientProtocol(w.size == numkeys, "WEIGHTS length must equal keys length")
        }
        get(dest, numkeys, keys, weights, aggregate)
    }
  }

  protected def findArgs(args: Seq[String], numkeys: Int) = {
    RequireClientProtocol(args != null && !args.isEmpty, "Args list must not be empty")
    args.head.toUpperCase match {
      case Commands.WEIGHTS => args.splitAt(numkeys+1)
      case Commands.AGGREGATE => args.splitAt(2)
      case s => throw ClientError("AGGREGATE or WEIGHTS argument expected, found %s".format(s))
    }
  }

  protected def findWeights(args0: Seq[String], args1: Seq[String]) = Weights(args0) match {
    case None => args1.length > 0 match {
      case true => Weights(args1) match {
        case None => throw ClientError("Have additional arguments but unable to process")
        case w => w
      }
      case false => None
    }
    case w => w
  }

  protected def findAggregate(args0: Seq[String], args1: Seq[String]) = Aggregate(args0) match {
    case None => args1.length > 0 match {
      case true => Aggregate(args1) match {
        case None => throw ClientError("Have additional arguments but unable to process")
        case agg => agg
      }
      case false => None
    }
    case agg => agg
  }
}


sealed trait ScoreCommand extends Command {
  val score: Double
}
sealed trait StrictScoreCommand extends ScoreCommand {
}

trait ZScoredRange extends KeyCommand { self =>
  val keyBuf: Buf
  val min: ZInterval
  val max: ZInterval
  val withScores: Option[CommandArgument]
  val limit: Option[Limit]

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  private[redis] def encoded: Seq[Buf]

  override def validate() {
    super.validate()
    withScores.foreach {
      case WithScores =>
      case _ => throw ClientError("withScores must be an instance of WithScores")
    }

    RequireClientProtocol(min != null, "min must not be null")
    RequireClientProtocol(max != null, "max must not be null")
  }
}
trait ZScoredRangeCompanion { self =>
  def get(
    key: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange

  def apply(args: Seq[Array[Byte]]) = args match {
    case key :: min :: max :: Nil =>
      get(Buf.ByteArray.Owned(key), ZInterval(min), ZInterval(max), None, None)
    case key :: min :: max :: tail =>
      parseArgs(Buf.ByteArray.Owned(key), ZInterval(min), ZInterval(max), tail)
    case _ =>
      throw ClientError("Expected either 3, 4 or 5 args for ZRANGEBYSCORE/ZREVRANGEBYSCORE")
  }

  def apply(key: Buf, min: ZInterval, max: ZInterval, withScores: CommandArgument) = {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), None)
      case _ =>
        throw ClientError("Only WITHSCORES is supported")
    }
  }

  def apply(key: Buf, min: ZInterval, max: ZInterval, limit: Limit) =
    get(key, min, max, None, Some(limit))

  def apply(key: Buf, min: ZInterval, max: ZInterval, withScores: CommandArgument,
            limit: Limit) =
  {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), Some(limit))
      case _ =>
        throw ClientError("Only WITHSCORES supported")
    }
  }

  protected def parseArgs(key: Buf, min: ZInterval, max: ZInterval, args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args != null && !args.isEmpty, "Expected arguments for command")
    val sArgs = BytesToString.fromList(args)
    val (arg0, remaining) = doParse(sArgs)

    remaining.isEmpty match {
      case true =>
        get(key, min, max, convertScore(arg0), convertLimit(arg0))
      case false =>
        val (arg1, leftovers) = doParse(remaining)
        leftovers.isEmpty match {
          case true =>
            val score = findScore(arg0, arg1)
            val limit = findLimit(arg0, arg1)
            get(key, min, max, score, limit)
          case false =>
            throw ClientError("Found unexpected extra arguments for command")
        }
    }
  }
  type ScoreOrLimit = Either[CommandArgument,Limit]
  protected def doParse(args: Seq[String]): (ScoreOrLimit, Seq[String]) = {
    args.head match {
      case WithScores(s) =>
        (Left(WithScores), if (args.length > 1) args.drop(1) else Nil)
      case _ =>
        (Right(Limit(args.take(3))), if (args.length > 3) args.drop(3) else Nil)
    }
  }
  protected def findScore(arg0: ScoreOrLimit, arg1: ScoreOrLimit) = convertScore(arg0) match {
    case None => convertScore(arg1) match {
      case None => throw ClientError("No WITHSCORES found but one expected")
      case s => s
    }
    case s => s
  }
  protected def convertScore(arg: ScoreOrLimit) = arg match {
    case Left(_) => Some(WithScores)
    case _ => None
  }
  protected def findLimit(arg0: ScoreOrLimit, arg1: ScoreOrLimit) = convertLimit(arg0) match {
    case None => convertLimit(arg1) match {
      case None => throw ClientError("No LIMIT found but one expected")
      case s => s
    }
    case s => s
  }
  protected def convertLimit(arg: ScoreOrLimit) = arg match {
    case Right(limit) => Some(limit)
    case _ => None
  }
}


abstract class ZRangeCmd extends StrictKeyCommand {
  def keyBuf: Buf
  val start: Long
  val stop: Long
  val withScores: Option[CommandArgument]

  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  private[redis] def encoded: Seq[Buf] = {
    def commands = Seq(
      keyBuf,
      Buf.Utf8(start.toString),
      Buf.Utf8(stop.toString)
    )

    val scored = withScores match {
      case Some(WithScores) => commands ++ WithScores.encoded
      case None => commands
    }

    scored
  }
}
trait ZRangeCmdCompanion {
  def get(key: Buf, start: Long, stop: Long, withScores: Option[CommandArgument]): ZRangeCmd

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(
      args != null && args.length >= 3,
      "Expected at least 3 arguments for command")

    val key = Buf.ByteArray.Owned(args.head)
    val rest = BytesToString.fromList(args.tail)

    rest match {
      case start :: stop :: Nil =>
        get(key, safeLong(start), safeLong(stop), None)
      case start :: stop :: withScores :: Nil =>
        withScores match {
          case WithScores(arg) =>
            get(key, safeLong(start), safeLong(stop), Some(WithScores))
          case _ =>
            throw ClientError("Expected 4 arguments with 4th as WITHSCORES")
        }
      case _ => throw ClientError("Expected 3 or 4 arguments for command")
    }
  }

  def apply(key: Buf, start: Long, stop: Long, scored: CommandArgument) = scored match {
    case WithScores => get(key, start, stop, Some(scored))
    case _ => throw ClientError("Only WithScores is supported")
  }

  protected def safeInt(i: String) = RequireClientProtocol.safe {
    NumberFormat.toInt(i)
  }

  protected def safeDouble(i: String) = RequireClientProtocol.safe {
    NumberFormat.toDouble(i)
  }

  protected def safeLong(i: String) = RequireClientProtocol.safe {
    NumberFormat.toLong(i)
  }
}


abstract class ZRankCmd extends StrictKeyCommand with StrictMemberCommand {
  val keyBuf: Buf
  val memberBuf: Buf
  def commandBytes: Buf

  override def member: ChannelBuffer = BufChannelBuffer(memberBuf)
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    commandBytes,
    keyBuf,
    memberBuf
  ))
}
trait ZRankCmdCompanion {
  def get(key: Buf, member: Buf): ZRankCmd

  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "ZRANKcmd")
    get(Buf.ByteArray.Owned(list(0)), Buf.ByteArray.Owned(list(1)))
  }
}
