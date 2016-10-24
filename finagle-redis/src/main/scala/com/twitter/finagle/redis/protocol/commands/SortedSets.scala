package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

case class ZAdd(key: Buf, members: Seq[ZMember])
  extends StrictKeyCommand
  with StrictZMembersCommand {

  def command: String = Commands.ZADD
  def toBuf: Buf = {
    val cmds = Seq(CommandBytes.ZADD, key)
    RedisCodec.toUnifiedBuf(cmds ++ membersWithScores)
  }
}

case class ZCard(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.ZCARD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.ZCARD, key))
}

case class ZCount(key: Buf, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  def command: String = Commands.ZCOUNT
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.ZCOUNT,
    key,
    Buf.Utf8(min.toString),
    Buf.Utf8(max.toString)
  ))
}

case class ZIncrBy(key: Buf, amount: Double, member: Buf)
  extends StrictKeyCommand
  with StrictMemberCommand {

  def command: String = Commands.ZINCRBY
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.ZINCRBY,
    key,
    Buf.Utf8(amount.toString),
    member
  ))
}

case class ZInterStore(
    destination: Buf,
    numkeys: Int,
    keys: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None) extends ZStore {

  validate()

  def command: String = Commands.ZINTERSTORE
  def commandBytes: Buf = CommandBytes.ZINTERSTORE
}

object ZInterStore {
  def apply(
    destination: Buf, keysBuf: Seq[Buf], weights: Weights
  ): ZInterStore = ZInterStore(destination, keysBuf.size, keysBuf, Some(weights), None)

  def apply(destination: Buf, keysBuf: Seq[Buf]): ZInterStore =
    ZInterStore(destination, keysBuf.size, keysBuf, None, None)

  def apply(destination: Buf, keysBuf: Seq[Buf], weights: Weights, aggregate: Aggregate): ZInterStore =
    ZInterStore(destination, keysBuf.size, keysBuf, Some(weights), Some(aggregate))
}

case class ZRange(
    key: Buf,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd {

  def command: String = Commands.ZRANGE
  def commandBytes: Buf = CommandBytes.ZRANGE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(commandBytes +: encoded)
}

object ZRange {
  def apply(key: Buf, start: Long, stop: Long, arg: CommandArgument): ZRange =
    ZRange(key, start, stop, Some(arg))
}

case class ZRangeByScore(
    key: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange {

  validate()

  def command: String = Commands.ZRANGEBYSCORE
  def commandBytes: Buf = CommandBytes.ZRANGEBYSCORE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(commandBytes +: encoded)

  private[redis] def encoded: Seq[Buf] = {
    def command = Seq(key, Buf.Utf8(min.value), Buf.Utf8(max.value))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }
}

object ZRangeByScore {
  def apply(keyBuf: Buf, min: ZInterval, max: ZInterval, limit: Limit): ZRangeByScore =
    ZRangeByScore(keyBuf, min, max, None, Some(limit))
}

case class ZRank(key: Buf, member: Buf) extends ZRankCmd {
  def command: String = Commands.ZRANK
  def commandBytes: Buf = CommandBytes.ZRANK
}

case class ZRem(key: Buf, members: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(
    members != null && members.nonEmpty,
    "Members list must not be empty for ZREM")

  def command: String = Commands.ZREM
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.ZREM, key) ++ members)
}

case class ZRemRangeByRank(key: Buf, start: Long, stop: Long) extends StrictKeyCommand {
  def command: String = Commands.ZREMRANGEBYRANK
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.ZREMRANGEBYRANK, key) ++ Seq(
      Buf.Utf8(start.toString),
      Buf.Utf8(stop.toString)
    )
  )
}

case class ZRemRangeByScore(key: Buf, min: ZInterval, max: ZInterval)
  extends StrictKeyCommand {

  def command: String = Commands.ZREMRANGEBYSCORE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(
    Seq(CommandBytes.ZREMRANGEBYSCORE, key) ++ Seq(
      Buf.Utf8(min.toString),
      Buf.Utf8(max.toString)
    )
  )
}

case class ZRevRange(
    key: Buf,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd {

  def command: String = Commands.ZREVRANGE
  def commandBytes: Buf = CommandBytes.ZREVRANGE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(commandBytes +: encoded)
}

case class ZRevRangeByScore(
    key: Buf,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange {

  validate()

  def command: String = Commands.ZREVRANGEBYSCORE
  def commandBytes: Buf = CommandBytes.ZREVRANGEBYSCORE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(commandBytes +: encoded)


  override private[redis] def encoded: Seq[Buf] = {
    val command = Seq(key, Buf.Utf8(max.toString), Buf.Utf8(min.toString))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }
}

case class ZRevRank(key: Buf, member: Buf) extends ZRankCmd {
  def command: String = Commands.ZREVRANK
  def commandBytes: Buf = CommandBytes.ZREVRANK
}

case class ZScore(key: Buf, member: Buf)
  extends StrictKeyCommand
  with StrictMemberCommand {

  def command: String = Commands.ZSCORE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.ZSCORE, key, member))
}

case class ZUnionStore(
    destination: Buf,
    numkeys: Int,
    keys: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore {

  validate()

  def command: String = Commands.ZUNIONSTORE
  def commandBytes: Buf = CommandBytes.ZUNIONSTORE
}

object ZUnionStore {
  def apply(destination: Buf, keysBuf: Seq[Buf], weights: Weights): ZUnionStore =
    ZUnionStore(destination, keysBuf.size, keysBuf, Some(weights), None)

  def apply(destination: Buf, keysBuf: Seq[Buf]): ZUnionStore =
    ZUnionStore(destination, keysBuf.size, keysBuf, None, None)

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

  override def toString: String = representation
}

object ZInterval {
  private val P_INF = "+inf"
  private val N_INF = "-inf"
  private val EXCLUSIVE = '('

  val MAX = new ZInterval(P_INF)
  val MIN = new ZInterval(N_INF)
  def apply(double: Double): ZInterval =
    new ZInterval(double.toString)
  def exclusive(double: Double): ZInterval =
    new ZInterval("%c%s".format(EXCLUSIVE, double.toString))
}

case class ZMember(score: Double, member: Buf)
  extends StrictScoreCommand
  with StrictMemberCommand {

  def command: String = "ZMEMBER"
  def toBuf: Buf = member
}


sealed trait ZMembersCommand {
  val members: Seq[ZMember]
}

sealed trait StrictZMembersCommand extends ZMembersCommand {
  RequireClientProtocol(members.nonEmpty, "Members set must not be empty")

  members.foreach { member =>
    RequireClientProtocol(member != null, "Empty member found")
  }

  def membersWithScores: Seq[Buf] = {
    members.flatMap(member => Seq(
      Buf.Utf8(member.score.toString),
      member.member
    ))
  }
}

/**
 * Helper Traits
 */

abstract class ZStore extends KeysCommand {
  val destination: Buf
  val numkeys: Int
  val keys: Seq[Buf]
  val weights: Option[Weights]
  val aggregate: Option[Aggregate]
  def commandBytes: Buf

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

  def toBuf: Buf = {
    var args = Seq(destination, Buf.Utf8(numkeys.toString)) ++ keys
    weights match {
      case Some(wlist) => args = args ++ wlist.encoded
      case None =>
    }
    aggregate match {
      case Some(agg) => args = args ++ agg.encoded
      case None =>
    }
    RedisCodec.toUnifiedBuf(commandBytes +: args)
  }
}

sealed trait ScoreCommand extends Command {
  val score: Double
}

sealed trait StrictScoreCommand extends ScoreCommand

trait ZScoredRange extends KeyCommand { self =>
  val min: ZInterval
  val max: ZInterval
  val withScores: Option[CommandArgument]
  val limit: Option[Limit]

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

abstract class ZRangeCmd extends StrictKeyCommand {
  val start: Long
  val stop: Long
  val withScores: Option[CommandArgument]

  private[redis] def encoded: Seq[Buf] = {
    def commands = Seq(
      key,
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

abstract class ZRankCmd extends StrictKeyCommand with StrictMemberCommand {
  def commandBytes: Buf

  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    commandBytes,
    key,
    member
  ))
}
