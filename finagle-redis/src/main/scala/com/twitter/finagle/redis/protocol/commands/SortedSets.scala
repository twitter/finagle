package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import java.lang.{Long => JLong}

case class ZAdd(key: Buf, members: Seq[ZMember]) extends StrictKeyCommand {
  RequireClientProtocol(members.nonEmpty, "Members set must not be empty")

  members.foreach { member => RequireClientProtocol(member != null, "Empty member found") }

  def name: Buf = Command.ZADD
  override def body: Seq[Buf] = {
    val membersWithScores =
      members.flatMap(member =>
        Seq(
          Buf.Utf8(member.score.toString),
          member.member
        ))

    key +: membersWithScores
  }
}

case class ZCard(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.ZCARD
}

case class ZCount(key: Buf, min: ZInterval, max: ZInterval) extends StrictKeyCommand {
  def name: Buf = Command.ZCOUNT
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(min.toString), Buf.Utf8(max.toString))
}

case class ZIncrBy(key: Buf, amount: Double, member: Buf)
    extends StrictKeyCommand
    with StrictMemberCommand {

  def name: Buf = Command.ZINCRBY
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(amount.toString), member)
}

case class ZInterStore(
  destination: Buf,
  numkeys: Int,
  keys: Seq[Buf],
  weights: Option[Weights] = None,
  aggregate: Option[Aggregate] = None)
    extends ZStore {

  validate()

  def name: Buf = Command.ZINTERSTORE
}

object ZInterStore {
  def apply(destination: Buf, keysBuf: Seq[Buf], weights: Weights): ZInterStore =
    ZInterStore(destination, keysBuf.size, keysBuf, Some(weights), None)

  def apply(destination: Buf, keysBuf: Seq[Buf]): ZInterStore =
    ZInterStore(destination, keysBuf.size, keysBuf, None, None)

  def apply(
    destination: Buf,
    keysBuf: Seq[Buf],
    weights: Weights,
    aggregate: Aggregate
  ): ZInterStore =
    ZInterStore(destination, keysBuf.size, keysBuf, Some(weights), Some(aggregate))
}

case class ZRange(key: Buf, start: Long, stop: Long, withScores: Option[CommandArgument] = None)
    extends ZRangeCmd {

  def name: Buf = Command.ZRANGE
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

  override def intervalBody: Seq[Buf] =
    Seq(Buf.Utf8(min.toString), Buf.Utf8(max.toString))

  def name: Buf = Command.ZRANGEBYSCORE
}

object ZRangeByScore {
  def apply(keyBuf: Buf, min: ZInterval, max: ZInterval, limit: Limit): ZRangeByScore =
    ZRangeByScore(keyBuf, min, max, None, Some(limit))
}

case class ZRank(key: Buf, member: Buf) extends ZRankCmd {
  def name: Buf = Command.ZRANK
}

case class ZRem(key: Buf, members: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(
    members != null && members.nonEmpty,
    "Members list must not be empty for ZREM"
  )

  def name: Buf = Command.ZREM
  override def body: Seq[Buf] = key +: members
}

case class ZRemRangeByRank(key: Buf, start: Long, stop: Long) extends StrictKeyCommand {
  def name: Buf = Command.ZREMRANGEBYRANK
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(start.toString), Buf.Utf8(stop.toString))
}

case class ZRemRangeByScore(key: Buf, min: ZInterval, max: ZInterval) extends StrictKeyCommand {

  def name: Buf = Command.ZREMRANGEBYSCORE
  override def body: Seq[Buf] = Seq(key, Buf.Utf8(min.toString), Buf.Utf8(max.toString))
}

case class ZRevRange(key: Buf, start: Long, stop: Long, withScores: Option[CommandArgument] = None)
    extends ZRangeCmd {

  def name: Buf = Command.ZREVRANGE
}

case class ZRevRangeByScore(
  key: Buf,
  max: ZInterval,
  min: ZInterval,
  withScores: Option[CommandArgument] = None,
  limit: Option[Limit] = None)
    extends ZScoredRange {

  validate()

  override def intervalBody: Seq[Buf] =
    Seq(Buf.Utf8(max.toString), Buf.Utf8(min.toString))

  def name: Buf = Command.ZREVRANGEBYSCORE
}

case class ZRevRank(key: Buf, member: Buf) extends ZRankCmd {
  def name: Buf = Command.ZREVRANK
}

case class ZScore(key: Buf, member: Buf) extends StrictKeyCommand with StrictMemberCommand {

  def name: Buf = Command.ZSCORE
  override def body: Seq[Buf] = Seq(key, member)
}

case class ZUnionStore(
  destination: Buf,
  numkeys: Int,
  keys: Seq[Buf],
  weights: Option[Weights] = None,
  aggregate: Option[Aggregate] = None)
    extends ZStore {

  validate()

  def name: Buf = Command.ZUNIONSTORE
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
      BytesToString(Buf.ByteArray.Owned.extract(score)).toDouble
    }

    ZRangeResults(arrays._1.toArray, doubles.toArray)
  }
}

/**
 * Represents part of an interval, helpers in companion object
 * See https://redis.io/commands/zrangebyscore for more info on different intervals
 */
case class ZInterval(value: String) {
  import ZInterval._
  private val representation = value.toLowerCase match {
    case N_INF => N_INF
    case P_INF => P_INF
    case double =>
      double.head match {
        case EXCLUSIVE =>
          RequireClientProtocol.safe {
            double.tail.toDouble
            double
          }
        case f =>
          RequireClientProtocol.safe {
            value.toDouble
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

/**
 * Helper Traits
 */
abstract class ZStore extends KeysCommand {
  val destination: Buf
  val numkeys: Int
  val keys: Seq[Buf]
  val weights: Option[Weights]
  val aggregate: Option[Aggregate]

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(destination.length > 0, "destination must not be empty")
    RequireClientProtocol(numkeys > 0, "numkeys must be > 0")
    RequireClientProtocol(keys.size == numkeys, "must supply the same number of keys as numkeys")
    // ensure if weights are specified they are equal to the size of numkeys
    weights match {
      case Some(list) =>
        RequireClientProtocol(
          list.size == numkeys,
          "If WEIGHTS specified, numkeys weights required"
        )
      case None =>
    }
  }

  override def body: Seq[Buf] = {
    var args = Seq(destination, Buf.Utf8(numkeys.toString)) ++ keys
    weights match {
      case Some(wlist) => args = args ++ (wlist.name +: wlist.body)
      case None =>
    }
    aggregate match {
      case Some(agg) => args = args ++ (agg.name +: agg.body)
      case None =>
    }

    args
  }
}

trait ZScoredRange extends KeyCommand { self =>
  def min: ZInterval
  def max: ZInterval
  def withScores: Option[CommandArgument]
  def limit: Option[Limit]

  protected def intervalBody: Seq[Buf]

  override def body: Seq[Buf] = {
    val command = key +: intervalBody
    val scores = withScores.map(ws => Seq(ws.name)).getOrElse(Nil)
    val limits = limit.map(l => l.name +: l.body).getOrElse(Nil)

    command ++ scores ++ limits
  }

  override def validate(): Unit = {
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

  override def body: Seq[Buf] = {
    val commands = Seq(
      key,
      Buf.Utf8(start.toString),
      Buf.Utf8(stop.toString)
    )

    withScores match {
      case Some(WithScores) => commands ++ (WithScores.name +: WithScores.body)
      case None => commands
    }
  }
}

abstract class ZRankCmd extends StrictKeyCommand with StrictMemberCommand {
  override def body: Seq[Buf] = Seq(key, member)
}

case class ZScan(key: Buf, cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
    extends Command {
  def name: Buf = Command.ZSCAN
  override def body: Seq[Buf] = {
    val bufs = Seq(key, Buf.Utf8(cursor.toString))
    val withCount = count match {
      case Some(count) => bufs ++ Seq(Command.COUNT, Buf.Utf8(count.toString))
      case None => bufs
    }
    pattern match {
      case Some(pattern) => withCount ++ Seq(Command.MATCH, pattern)
      case None => withCount
    }
  }
}

case class ZPopMin(key: Buf, count: Option[JLong] = None) extends Command {
  def name: Buf = Command.ZPOPMIN
  override def body: Seq[Buf] = {
    count match {
      case Some(count) => Seq(key, Buf.Utf8(count.toString))
      case None => Seq(key)
    }
  }
}

case class ZPopMax(key: Buf, count: Option[JLong] = None) extends Command {
  def name: Buf = Command.ZPOPMAX
  override def body: Seq[Buf] = {
    count match {
      case Some(count) => Seq(key, Buf.Utf8(count.toString))
      case None => Seq(key)
    }
  }
}
