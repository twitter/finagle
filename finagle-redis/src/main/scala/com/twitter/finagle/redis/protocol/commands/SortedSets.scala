package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
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

case class ZCard(keyBuf: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.ZCARD
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.ZCARD, keyBuf))
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

case class ZInterStore(
    destination: Buf,
    numkeys: Int,
    keysBuf: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None) extends ZStore {

  validate()

  def command = Commands.ZINTERSTORE
  def commandBytes = CommandBytes.ZINTERSTORE
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
    keyBuf: Buf,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd {

  def command = Commands.ZRANGE
  def commandBytes = CommandBytes.ZRANGE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)
}

object ZRange {
  def apply(key: Buf, start: Long, stop: Long, arg: CommandArgument): ZRange =
    ZRange(key, start, stop, Some(arg))
}

case class ZRangeByScore(
    keyBuf: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange {

  validate()

  def command = Commands.ZRANGEBYSCORE
  def commandBytes = CommandBytes.ZRANGEBYSCORE

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)

  private[redis] def encoded: Seq[Buf] = {
    def command = Seq(keyBuf, Buf.Utf8(min.value), Buf.Utf8(max.value))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }
}

object ZRangeByScore {
  def apply(keyBuf: Buf, min: ZInterval, max: ZInterval, limit: Limit): ZRangeByScore =
    ZRangeByScore(keyBuf, min, max, None, Some(limit))
}

case class ZRank(keyBuf: Buf, memberBuf: Buf) extends ZRankCmd {
  def command = Commands.ZRANK
  def commandBytes = CommandBytes.ZRANK
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

case class ZRevRange(
    keyBuf: Buf,
    start: Long,
    stop: Long,
    withScores: Option[CommandArgument] = None)
  extends ZRangeCmd {

  def command = Commands.ZREVRANGE
  def commandBytes = CommandBytes.ZREVRANGE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)
}

case class ZRevRangeByScore(
    keyBuf: Buf,
    max: ZInterval,
    min: ZInterval,
    withScores: Option[CommandArgument] = None,
    limit: Option[Limit] = None)
  extends ZScoredRange {

  validate()

  def command = Commands.ZREVRANGEBYSCORE
  def commandBytes = CommandBytes.ZREVRANGEBYSCORE

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(commandBytes +: encoded)


  override private[redis] def encoded: Seq[Buf] = {
    def command = Seq(keyBuf, Buf.Utf8(max.toString), Buf.Utf8(min.toString))
    val scores = withScores.map(_.encoded).getOrElse(Nil)
    val limits = limit.map(_.encoded).getOrElse(Nil)

    command ++ scores ++ limits
  }
}

case class ZRevRank(keyBuf: Buf, memberBuf: Buf) extends ZRankCmd {
  def command = Commands.ZREVRANK
  def commandBytes = CommandBytes.ZREVRANK
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

case class ZUnionStore(
    destination: Buf,
    numkeys: Int,
    keysBuf: Seq[Buf],
    weights: Option[Weights] = None,
    aggregate: Option[Aggregate] = None)
  extends ZStore {

  validate()

  def command = Commands.ZUNIONSTORE
  def commandBytes = CommandBytes.ZUNIONSTORE
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

  override def toString = representation
}

object ZInterval {
  private val P_INF = "+inf"
  private val N_INF = "-inf"
  private val EXCLUSIVE = '('

  val MAX = new ZInterval(P_INF)
  val MIN = new ZInterval(N_INF)
  def apply(double: Double) = new ZInterval(double.toString)
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

sealed trait ScoreCommand extends Command {
  val score: Double
}

sealed trait StrictScoreCommand extends ScoreCommand

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
