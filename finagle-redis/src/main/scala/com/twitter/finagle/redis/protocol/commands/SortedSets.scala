package com.twitter.finagle.redis
package protocol

import util._
import Commands.trimList

case class ZAdd(key: String, members: List[ZMember])
  extends StrictKeyCommand
  with StrictZMembersCommand
{
  override def toChannelBuffer = {
    val cmds = StringToBytes.fromList(List(Commands.ZADD, key))
    RedisCodec.toUnifiedFormat(cmds ::: membersByteArray)
  }
}
object ZAdd {
  def apply(args: List[Array[Byte]]) = args match {
    case head :: tail =>
      new ZAdd(BytesToString(head), ZMembers(tail))
    case _ =>
      throw ClientError("Invalid use of ZADD")
  }
  def apply(key: String, member: ZMember) = new ZAdd(key, List(member))
}


case class ZCard(key: String) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toInlineFormat(List(Commands.ZCARD, key))
}
object ZCard {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 1, "ZCARD"))
    new ZCard(list(0))
  }
}


case class ZCount(key: String, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.ZCOUNT, key, min.toString, max.toString))
}
object ZCount {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZCOUNT"))
    new ZCount(list(0), ZInterval(list(1)), ZInterval(list(2)))
  }
}


case class ZIncrBy(key: String, amount: Float, member: Array[Byte])
  extends StrictKeyCommand
  with StrictMemberCommand
{
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(
      StringToBytes(Commands.ZINCRBY),
      StringToBytes(key),
      StringToBytes(amount.toString),
      member))
}
object ZIncrBy {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 3, "ZINCRBY")
    val key = BytesToString(list(0))
    val amount = RequireClientProtocol.safe {
      NumberFormat.toFloat(BytesToString(list(1)))
    }
    new ZIncrBy(key, amount, list(2))
  }
}


case class ZInterStore(
    destination: String,
    numkeys: Int,
    keys: List[String],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore
{
  val command = Commands.ZINTERSTORE
  validate()
}
object ZInterStore extends ZStoreCompanion {
  def get(
    dest: String,
    numkeys: Int,
    keys: List[String],
    weights: Option[Weights],
    agg: Option[Aggregate]) = new ZInterStore(dest, numkeys, keys, weights, agg)
}


case class ZRange(key: String, start: Int, stop: Int, withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  val command = Commands.ZRANGE
}
object ZRange extends ZRangeCmdCompanion {
  override def get(key: String, start: Int, stop: Int, withScores: Option[CommandArgument]) =
    new ZRange(key, start, stop, withScores)
}


case class ZRangeByScore(
    key: String,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange
{
  val command = Commands.ZRANGEBYSCORE
  validate()
}
object ZRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: String,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange = new ZRangeByScore(key, min, max, withScores, limit)
}


case class ZRank(key: String, member: Array[Byte]) extends ZRankCmd {
  val command = Commands.ZRANK
}
object ZRank extends ZRankCmdCompanion {
  def get(key: String, member: Array[Byte]) = new ZRank(key, member)
}


case class ZRem(key: String, members: List[Array[Byte]]) extends StrictKeyCommand {
  RequireClientProtocol(
    members != null && members.length > 0,
    "Members list must not be empty for ZREM")

  override def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(
      List(StringToBytes(Commands.ZREM), StringToBytes(key)) ::: members
    )
  }
}
object ZRem {
  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.length > 1, "ZREM requires at least one member")
    val key = BytesToString(args(0))
    val remaining = args.drop(1)
    new ZRem(key, remaining)
  }
}


case class ZRemRangeByRank(key: String, start: Int, stop: Int) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(
    Commands.ZREMRANGEBYRANK,
    key,
    start.toString,
    stop.toString)))
}
object ZRemRangeByRank {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYRANK requires 3 arguments"))
    val key = list(0)
    val start = RequireClientProtocol.safe { NumberFormat.toInt(list(1)) }
    val stop = RequireClientProtocol.safe { NumberFormat.toInt(list(2)) }
    new ZRemRangeByRank(key, start, stop)
  }
}


case class ZRemRangeByScore(key: String, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(StringToBytes.fromList(List(
    Commands.ZREMRANGEBYSCORE,
    key,
    min.toString,
    max.toString)))
}
object ZRemRangeByScore {
  def apply(args: List[Array[Byte]]) = {
    val list = BytesToString.fromList(trimList(args, 3, "ZREMRANGEBYSCORE requires 3 arguments"))
    val key = list(0)
    val min = ZInterval(list(1))
    val max = ZInterval(list(2))
    new ZRemRangeByScore(key, min, max)
  }
}


case class ZRevRange(
    key: String,
    start: Int,
    stop: Int,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd
{
  val command = Commands.ZREVRANGE
}
object ZRevRange extends ZRangeCmdCompanion {
  override def get(key: String, start: Int, stop: Int, withScores: Option[CommandArgument]) =
    new ZRevRange(key, start, stop, withScores)
}


case class ZRevRangeByScore(
    key: String,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange
{
  val command = Commands.ZREVRANGEBYSCORE
  validate()
}
object ZRevRangeByScore extends ZScoredRangeCompanion {
  def get(
    key: String,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange = new ZRevRangeByScore(key, max, min, withScores, limit)
}


case class ZRevRank(key: String, member: Array[Byte]) extends ZRankCmd {
  val command = Commands.ZREVRANK
}
object ZRevRank extends ZRankCmdCompanion {
  def get(key: String, member: Array[Byte]) = new ZRevRank(key, member)
}


case class ZScore(key: String, member: Array[Byte])
  extends StrictKeyCommand
  with StrictMemberCommand
{
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
    StringToBytes(Commands.ZSCORE),
    StringToBytes(key),
    member))
}
object ZScore {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "ZSCORE")
    new ZScore(BytesToString(args(0)), args(1))
  }
}


case class ZUnionStore(
    destination: String,
    numkeys: Int,
    keys: List[String],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore
{
  val command = Commands.ZUNIONSTORE
  validate()
}
object ZUnionStore extends ZStoreCompanion {
  def get(
    dest: String,
    numkeys: Int,
    keys: List[String],
    weights: Option[Weights],
    agg: Option[Aggregate]) = new ZUnionStore(dest, numkeys, keys, weights, agg)
}

/**
 * Internal Helpers
 */

// Represents part of an interval, helpers in companion object
case class ZInterval(value: String) {
  import ZInterval._
  private val representation = value.toLowerCase match {
    case N_INF => N_INF
    case P_INF => P_INF
    case float => float.head match {
      case EXCLUSIVE => RequireClientProtocol.safe {
        NumberFormat.toFloat(float.tail)
        float
      }
      case f => RequireClientProtocol.safe {
        NumberFormat.toFloat(value)
        float
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
  def apply(float: Float) = new ZInterval(float.toString)
  def apply(v: Array[Byte]) = new ZInterval(BytesToString(v))
  def exclusive(float: Float) = new ZInterval("%c%s".format(EXCLUSIVE, float.toString))
}


case class ZMember(score: Float, member: Array[Byte])
  extends StrictScoreCommand
  with StrictMemberCommand
{
  override def toChannelBuffer =
    throw new UnsupportedOperationException("ZMember doesn't support toChannelBuffer")
}


sealed trait ScoreCommand extends Command {
  val score: Float
}
sealed trait StrictScoreCommand extends ScoreCommand {
}


sealed trait ZMembersCommand {
  val members: List[ZMember]
}
sealed trait StrictZMembersCommand extends ZMembersCommand {
  RequireClientProtocol(members != null && members.length > 0, "Members set must not be empty")
  members.foreach { member =>
    RequireClientProtocol(member != null, "Empty member found")
  }
  def membersByteArray: List[Array[Byte]] = {
    members.map { member =>
      List(
        StringToBytes(member.score.toString),
        member.member
      )
    }.flatten
  }
}


object ZMembers {
  def apply(args: List[Array[Byte]]): List[ZMember] = {
    val size = args.length
    RequireClientProtocol(size % 2 == 0 && size > 0, "Unexpected uneven pair of elements")

    args.grouped(2).map {
      case score :: member :: Nil =>
        ZMember(
          RequireClientProtocol.safe {
            NumberFormat.toFloat(BytesToString(score))
          },
          member)
      case _ =>
        throw ClientError("Unexpected uneven pair of elements in members")
    }.toList
  }
}


abstract class ZStore extends KeysCommand {
  val command: String
  val destination: String
  val numkeys: Int
  val keys: List[String]
  val weights: Option[Weights]
  val aggregate: Option[Aggregate]

  override protected def validate() {
    super.validate()
    RequireClientProtocol(
      destination != null && destination.length > 0,
      "destination must not be empty")
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

  override def toChannelBuffer = {
    // FIXME
    var args = List(command, destination, numkeys.toString) ::: keys
    weights match {
      case Some(wlist) => args = args :+ wlist.toString
      case None =>
    }
    aggregate match {
      case Some(agg) => args = args :+ agg.toString
      case None =>
    }
    RedisCodec.toInlineFormat(args)
  }
}
trait ZStoreCompanion {
  def apply(dest: String, keys: List[String]) = get(dest, keys.length, keys, None, None)
  def apply(dest: String, keys: List[String], weights: Weights) = {
    get(dest, keys.length, keys, Some(weights), None)
  }
  def apply(dest: String, keys: List[String], agg: Aggregate) =
    get(dest, keys.length, keys, None, Some(agg))
  def apply(dest: String, keys: List[String], weights: Weights, agg: Aggregate) =
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
  def get(d: String, n: Int, k: List[String], w: Option[Weights], a: Option[Aggregate]): ZStore

  def apply(args: List[Array[Byte]]) = BytesToString.fromList(args) match {
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

  protected def parseArgs(dest: String, numkeys: Int, remaining: List[String]) = {
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

  protected def findArgs(args: List[String], numkeys: Int) = {
    RequireClientProtocol(args != null && args.length > 0, "Args list must not be empty")
    args.head.toUpperCase match {
      case Weights.WEIGHTS => args.splitAt(numkeys+1)
      case Aggregate.AGGREGATE => args.splitAt(2)
      case s => throw ClientError("AGGREGATE or WEIGHTS argument expected, found %s".format(s))
    }
  }

  protected def findWeights(args0: List[String], args1: List[String]) = Weights(args0) match {
    case None => args1.length > 0 match {
      case true => Weights(args1) match {
        case None => throw ClientError("Have additional arguments but unable to process")
        case w => w
      }
      case false => None
    }
    case w => w
  }

  protected def findAggregate(args0: List[String], args1: List[String]) = Aggregate(args0) match {
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


trait ZScoredRange extends KeyCommand { self =>
  val key: String
  val min: ZInterval
  val max: ZInterval
  val withScores: Option[CommandArgument]
  val limit: Option[Limit]

  val command: String

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

  override def toChannelBuffer = {
    val command = List(self.command, key, min.toString, max.toString)
    val scores: List[String] = withScores match {
      case Some(WithScores) => List(WithScores.toString)
      case None => Nil
    }
    val limits: List[String] = limit match {
      case Some(limit) => List(limit.toString)
      case None => Nil
    }
    RedisCodec.toInlineFormat(command ::: scores ::: limits)
  }
}
trait ZScoredRangeCompanion { self =>
  def get(
    key: String,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument],
    limit: Option[Limit]): ZScoredRange

  def apply(args: List[Array[Byte]]) = args match {
    case key :: min :: max :: Nil =>
      get(BytesToString(key), ZInterval(min), ZInterval(max), None, None)
    case key :: min :: max :: tail =>
      parseArgs(BytesToString(key), ZInterval(min), ZInterval(max), tail)
    case _ =>
      throw ClientError("Expected either 3, 4 or 5 args for ZRANGEBYSCORE/ZREVRANGEBYSCORE")
  }

  def apply(key: String, min: ZInterval, max: ZInterval, withScores: CommandArgument) = {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), None)
      case _ =>
        throw ClientError("Only WITHSCORES is supported")
    }
  }

  def apply(key: String, min: ZInterval, max: ZInterval, limit: Limit) =
    get(key, min, max, None, Some(limit))

  def apply(key: String, min: ZInterval, max: ZInterval, withScores: CommandArgument,
            limit: Limit) =
  {
    withScores match {
      case WithScores =>
        get(key, min, max, Some(withScores), Some(limit))
      case _ =>
        throw ClientError("Only WITHSCORES supported")
    }
  }

  protected def parseArgs(key: String, min: ZInterval, max: ZInterval, args: List[Array[Byte]]) = {
    RequireClientProtocol(args != null && args.length > 0, "Expected arguments for command")
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
  protected def doParse(args: List[String]): (ScoreOrLimit, List[String]) = {
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
  val key: String
  val start: Int
  val stop: Int
  val withScores: Option[CommandArgument]
  val command: String

  private def forChannelBuffer = {
    val commands = List(this.command, key, start.toString, stop.toString)
    val scored = withScores match {
      case Some(WithScores) => commands :+ WithScores.toString
      case None => commands
    }
    StringToBytes.fromList(scored)
  }
  override def toChannelBuffer = RedisCodec.toUnifiedFormat(forChannelBuffer)

}
trait ZRangeCmdCompanion {
  def get(key: String, start: Int, stop: Int, withScores: Option[CommandArgument]): ZRangeCmd

  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(
      args != null && args.length >= 3,
      "Expected at least 3 arguments for command")

    BytesToString.fromList(args) match {
      case key :: start :: stop :: Nil =>
        get(key, safeInt(start), safeInt(stop), None)
      case key :: start :: stop :: withScores :: Nil =>
        withScores match {
          case WithScores(arg) =>
            get(key, safeInt(start), safeInt(stop), Some(WithScores))
          case _ =>
            throw ClientError("Expected 4 arguments with 4th as WITHSCORES")
        }
      case _ => throw ClientError("Expected 3 or 4 arguments for command")
    }
  }

  def apply(key: String, start: Int, stop: Int, scored: CommandArgument) = scored match {
    case WithScores => get(key, start, stop, Some(scored))
    case _ => throw ClientError("Only WithScores is supported")
  }

  protected def safeInt(i: String) = RequireClientProtocol.safe {
    NumberFormat.toInt(i)
  }
}


abstract class ZRankCmd extends StrictKeyCommand with StrictMemberCommand {
  val key: String
  val member: Array[Byte]
  val command: String

  override def toChannelBuffer = RedisCodec.toUnifiedFormat(List(
      StringToBytes(this.command),
      StringToBytes(key),
      member))
}
trait ZRankCmdCompanion {
  def get(key: String, member: Array[Byte]): ZRankCmd

  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "ZRANKcmd")
    get(BytesToString(args(0)), args(1))
  }
}
