package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class ZAdd(key: ChannelBuffer, members: Seq[ZMember])
  extends StrictKeyCommand
  with StrictZMembersCommand
{
  def command = Commands.ZADD
  def toChannelBuffer = {
    val cmds = Seq(CommandBytes.ZADD, key)
    RedisCodec.toUnifiedFormat(cmds ++ membersChannelBuffers)
  }
}
object ZAdd {
  def apply(args: Seq[Array[Byte]]) = args match {
    case head :: tail =>
      new ZAdd(ChannelBuffers.wrappedBuffer(head), ZMembers(tail))
    case _ =>
      throw ClientError("Invalid use of ZADD")
  }
}

case class ZCard(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.ZCARD
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.ZCARD, key))
}
object ZCard {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "ZCARD requires at least one member")
    new ZCard(ChannelBuffers.wrappedBuffer(args(0)))
  }
}

case class ZCount(key: ChannelBuffer, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  def command = Commands.ZCOUNT
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(
      CommandBytes.ZCOUNT,
      key,
      StringToChannelBuffer(min.toString),
      StringToChannelBuffer(max.toString)
    ))
}
object ZCount {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZCOUNT"))
    new ZCount(ChannelBuffers.wrappedBuffer(args(0)), ZInterval(list(1)), ZInterval(list(2)))
  }
}

case class ZIncrBy(key: ChannelBuffer, amount: Double, member: ChannelBuffer)
  extends StrictKeyCommand
  with StrictMemberCommand
{
  def command = Commands.ZINCRBY
  def toChannelBuffer =
    RedisCodec.toUnifiedFormat(Seq(
      CommandBytes.ZINCRBY,
      key,
      StringToChannelBuffer(amount.toString),
      member))
}
object ZIncrBy {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "ZINCRBY")
    val key = list(0)
    val amount = RequireClientProtocol.safe {
      NumberFormat.toDouble(BytesToString(list(1)))
    }
    new ZIncrBy(
      ChannelBuffers.wrappedBuffer(key),
      amount,
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}

case class ZInterStore(
    destination: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer],
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
        StringToChannelBuffer(dest),
        numkeys,
        keys.map(StringToChannelBuffer(_)),
        weights,
        agg)
}

case class ZRange(key: ChannelBuffer, start: Long, stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  def command = Commands.ZRANGE
  def commandBytes = CommandBytes.ZRANGE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(commandBytes +: forChannelBuffer)
}
object ZRange extends ZRangeCmdCompanion {
  override def get(key: ChannelBuffer, start: Long, stop: Long, withScores: Option[CommandArgument]) =
    new ZRange(key, start, stop, withScores)
}

case class ZRangeByScore(
    key: ChannelBuffer,
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
    RedisCodec.toUnifiedFormat(commandBytes +: forChannelBuffer)

  def forChannelBuffer = {
    def command = Seq(key, min.toChannelBuffer, max.toChannelBuffer)
    val scores: Seq[ChannelBuffer] = withScores match {
      case Some(WithScores) => Seq(WithScores.toChannelBuffer)
      case None => Nil
    }
    val limits: Seq[ChannelBuffer] = limit match {
      case Some(limit) => limit.toChannelBuffers
      case None => Nil
    }
    (command ++ scores ++ limits)
  }
}
object ZRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: ChannelBuffer,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange =
      new ZRangeByScore(key, min, max, withScores, limit)
}

case class ZRank(key: ChannelBuffer, member: ChannelBuffer) extends ZRankCmd {
  def command = Commands.ZRANK
  def commandBytes = CommandBytes.ZRANK
}
object ZRank extends ZRankCmdCompanion {
  def get(key: ChannelBuffer, member: ChannelBuffer) =
    new ZRank(key, member)
}

case class ZRem(key: ChannelBuffer, members: Seq[ChannelBuffer]) extends StrictKeyCommand {
  RequireClientProtocol(
    members != null && members.length > 0,
    "Members list must not be empty for ZREM")
  def command = Commands.ZREM
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.ZREM, key) ++ members)
}
object ZRem {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.length > 1, "ZREM requires at least one member")
    val key = ChannelBuffers.wrappedBuffer(args(0))
    val remaining = args.drop(1).map(ChannelBuffers.wrappedBuffer(_))
    new ZRem(key, remaining)
  }
}

case class ZRemRangeByRank(key: ChannelBuffer, start: Long, stop: Long) extends StrictKeyCommand {
  def command = Commands.ZREMRANGEBYRANK
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
    Seq(CommandBytes.ZREMRANGEBYRANK, key) ++ Seq(
      StringToChannelBuffer(start.toString),
      StringToChannelBuffer(stop.toString)))
}
object ZRemRangeByRank {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYRANK requires 3 arguments"))
    val key = ChannelBuffers.wrappedBuffer(args(0))
    val start = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val stop = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new ZRemRangeByRank(key, start, stop)
  }
}

case class ZRemRangeByScore(key: ChannelBuffer, min: ZInterval, max: ZInterval)
  extends StrictKeyCommand {
  def command = Commands.ZREMRANGEBYSCORE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
    Seq(CommandBytes.ZREMRANGEBYSCORE, key) ++ Seq(
      StringToChannelBuffer(min.toString),
      StringToChannelBuffer(max.toString)))
}
object ZRemRangeByScore {
  def apply(args: Seq[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYSCORE requires 3 arguments"))
    val key = ChannelBuffers.wrappedBuffer(args(0))
    val min = ZInterval(list(1))
    val max = ZInterval(list(2))
    new ZRemRangeByScore(key, min, max)
  }
}

case class ZRevRange(
    key: ChannelBuffer,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  def command = Commands.ZREVRANGE
  def commandBytes = CommandBytes.ZREVRANGE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(commandBytes +: forChannelBuffer)
}
object ZRevRange extends ZRangeCmdCompanion {
  override def get(key: ChannelBuffer, start: Long, stop: Long, withScores: Option[CommandArgument]) =
    new ZRevRange(key, start, stop, withScores)
}

case class ZRevRangeByScore(
    key: ChannelBuffer,
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
    RedisCodec.toUnifiedFormat(commandBytes +: forChannelBuffer)

  def forChannelBuffer = {
    def command = Seq(key, max.toChannelBuffer, min.toChannelBuffer)
    val scores: Seq[ChannelBuffer] = withScores match {
      case Some(WithScores) => Seq(WithScores.toChannelBuffer)
      case None => Nil
    }
    val limits: Seq[ChannelBuffer] = limit match {
      case Some(limit) => limit.toChannelBuffers
      case None => Nil
    }
    (command ++ scores ++ limits)
  }

}
object ZRevRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: ChannelBuffer,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange =
      new ZRevRangeByScore(key, max, min, withScores, limit)
}

case class ZRevRank(key: ChannelBuffer, member: ChannelBuffer) extends ZRankCmd {
  def command = Commands.ZREVRANK
  def commandBytes = CommandBytes.ZREVRANK
}
object ZRevRank extends ZRankCmdCompanion {
  def get(key: ChannelBuffer, member: ChannelBuffer) =
    new ZRevRank(key, member)
}

case class ZScore(key: ChannelBuffer, member: ChannelBuffer)
  extends StrictKeyCommand
  with StrictMemberCommand
{
  def command = Commands.ZSCORE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
    CommandBytes.ZSCORE,
    key,
    member))
}
object ZScore {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "ZSCORE")
    new ZScore(ChannelBuffers.wrappedBuffer(args(0)), ChannelBuffers.wrappedBuffer(args(1)))
  }
}

case class ZUnionStore(
    destination: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer],
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
      new ZUnionStore(StringToChannelBuffer(dest),
        numkeys,
        keys.map(StringToChannelBuffer(_)),
        weights,
        agg)
}

/**
 * Helper Objects
 */

case class ZRangeResults(entries: Array[ChannelBuffer], scores: Array[Double]) {
  def asTuples(): Seq[(ChannelBuffer, Double)] =
    (entries, scores).zipped.map { (entry, score) => (entry, score) }.toSeq
}
object ZRangeResults {
  def apply(tuples: Seq[(ChannelBuffer, ChannelBuffer)]): ZRangeResults = {
    val arrays = tuples.unzip
    val doubles = arrays._2 map { score => NumberFormat.toDouble(BytesToString(score.array)) }
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
  override def toString = value
  def toChannelBuffer = StringToChannelBuffer(value)
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

case class ZMember(score: Double, member: ChannelBuffer)
  extends StrictScoreCommand
  with StrictMemberCommand
{
  def command = "ZMEMBER"
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
        member.member.array
      )
    }.flatten
  }
  def membersChannelBuffers: Seq[ChannelBuffer] = {
    members.map { member =>
      Seq(
        StringToChannelBuffer(member.score.toString),
        member.member
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
          ChannelBuffers.wrappedBuffer(member))
      case _ =>
        throw ClientError("Unexpected uneven pair of elements in members")
    }.toSeq
  }
}

/**
 * Helper Traits
 */

abstract class ZStore extends KeysCommand {
  val destination: ChannelBuffer
  val numkeys: Int
  val keys: Seq[ChannelBuffer]
  val weights: Option[Weights]
  val aggregate: Option[Aggregate]
  def commandBytes: ChannelBuffer

  override protected def validate() {
    super.validate()
    RequireClientProtocol(destination.readableBytes > 0, "destination must not be empty")
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
    var args = Seq(destination, StringToChannelBuffer(numkeys.toString)) ++ keys
    weights match {
      case Some(wlist) => args = args ++ wlist.toChannelBuffers
      case None =>
    }
    aggregate match {
      case Some(agg) => args = args ++ agg.toChannelBuffers
      case None =>
    }
    RedisCodec.toUnifiedFormat(commandBytes +: args)
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
      case Weights.WEIGHTS => args.splitAt(numkeys+1)
      case Aggregate.AGGREGATE => args.splitAt(2)
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
  val key: ChannelBuffer
  val min: ZInterval
  val max: ZInterval
  val withScores: Option[CommandArgument]
  val limit: Option[Limit]

  def forChannelBuffer: Seq[ChannelBuffer]

  override def validate() {
    super.validate()
    withScores.map { s =>
      s match {
        case WithScores =>
        case _ => throw ClientError("withScores must be an instance of WithScores")
      }
    }
    RequireClientProtocol(min != null, "min must not be null")
    RequireClientProtocol(max != null, "max must not be null")
  }
}
trait ZScoredRangeCompanion { self =>
  def get(
    key: ChannelBuffer,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange

  def apply(args: Seq[Array[Byte]]) = args match {
    case key :: min :: max :: Nil =>
      get(ChannelBuffers.wrappedBuffer(key), ZInterval(min), ZInterval(max), None, None)
    case key :: min :: max :: tail =>
      parseArgs(ChannelBuffers.wrappedBuffer(key), ZInterval(min), ZInterval(max), tail)
    case _ =>
      throw ClientError("Expected either 3, 4 or 5 args for ZRANGEBYSCORE/ZREVRANGEBYSCORE")
  }

  def apply(key: ChannelBuffer, min: ZInterval, max: ZInterval, withScores: CommandArgument) = {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), None)
      case _ =>
        throw ClientError("Only WITHSCORES is supported")
    }
  }

  def apply(key: ChannelBuffer, min: ZInterval, max: ZInterval, limit: Limit) =
    get(key, min, max, None, Some(limit))

  def apply(key: ChannelBuffer, min: ZInterval, max: ZInterval, withScores: CommandArgument,
            limit: Limit) =
  {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), Some(limit))
      case _ =>
        throw ClientError("Only WITHSCORES supported")
    }
  }

  protected def parseArgs(key: ChannelBuffer, min: ZInterval, max: ZInterval, args: Seq[Array[Byte]]) = {
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
  val key: ChannelBuffer
  val start: Long
  val stop: Long
  val withScores: Option[CommandArgument]

  def forChannelBuffer = {
    def commands = Seq(key, StringToChannelBuffer(start.toString),
      StringToChannelBuffer(stop.toString))
    val scored = withScores match {
      case Some(WithScores) => commands :+ WithScores.toChannelBuffer
      case None => commands
    }
    scored
  }
}
trait ZRangeCmdCompanion {
  def get(key: ChannelBuffer, start: Long, stop: Long, withScores: Option[CommandArgument]): ZRangeCmd

  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(
      args != null && args.length >= 3,
      "Expected at least 3 arguments for command")

    val key = ChannelBuffers.wrappedBuffer(args.head)
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

  def apply(key: ChannelBuffer, start: Long, stop: Long, scored: CommandArgument) = scored match {
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
  val key: ChannelBuffer
  val member: ChannelBuffer
  def commandBytes: ChannelBuffer

  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(
      this.commandBytes,
      key,
      member))
}
trait ZRankCmdCompanion {
  def get(key: ChannelBuffer, member: ChannelBuffer): ZRankCmd

  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "ZRANKcmd")
    get(ChannelBuffers.wrappedBuffer(args(0)), ChannelBuffers.wrappedBuffer(args(1)))
  }
}
