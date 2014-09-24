package com.twitter.finagle.redis.protocol

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.ChannelBuffers

trait CommandArgument extends Command

case object WithScores extends CommandArgument {
  def command = "WITHSCORES"
  val WITHSCORES = command
  def commandBytes = StringToChannelBuffer(command)
  def unapply(s: String) = s.toUpperCase match {
    case WITHSCORES => Some(s)
    case _ => None
  }
  override def toString = command
  def toChannelBuffer = commandBytes
  @deprecated("Prefer option") val asArg = Some(WithScores)
  def option(opt: Boolean): Option[this.type] = if (opt) asArg else None
}

case class Limit(offset: Long, count: Long) extends CommandArgument {
  def command = Limit.LIMIT
  def commandBytes = Limit.LIMIT_CB
  override def toString = "%s %d %d".format(Limit.LIMIT, offset, count)
  def toChannelBuffer = ChannelBuffers.wrappedBuffer(toChannelBuffers.toArray:_*)
  def toChannelBuffers = Seq(Limit.LIMIT_CB,
    StringToChannelBuffer(offset.toString), StringToChannelBuffer(count.toString))
}
object Limit {
  val LIMIT = "LIMIT"
  val LIMIT_CB = StringToChannelBuffer(LIMIT)
  def apply(args: Seq[String]) = {
    RequireClientProtocol(args != null && args.length == 3, "LIMIT requires two arguments")
    RequireClientProtocol(args.head == LIMIT, "LIMIT must start with LIMIT clause")
    RequireClientProtocol.safe {
      val offset = NumberFormat.toLong(args(1))
      val count = NumberFormat.toLong(args(2))
      new Limit(offset, count)
    }
  }
}

// Represents a list of WEIGHTS
class Weights(underlying: Array[Double]) extends CommandArgument with IndexedSeq[Double] {
  def apply(idx: Int) = underlying(idx)
  def length = underlying.length
  override def toString = Weights.toString + " " + this.mkString(" ")
  def toChannelBuffer = ChannelBuffers.wrappedBuffer(toChannelBuffers.toArray:_*)
  def toChannelBuffers =
    Weights.WEIGHTS_CB +: underlying.map(w => StringToChannelBuffer(w.toString)).toSeq
  def command = Weights.WEIGHTS
}

// Handles parsing and manipulation of WEIGHTS arguments
object Weights {
  val WEIGHTS = "WEIGHTS"
  val WEIGHTS_CB = StringToChannelBuffer(WEIGHTS)

  def apply(weight: Double) = new Weights(Array(weight))
  def apply(weights: Double*) = new Weights(weights.toArray)
  def apply(weights: Array[Double]) = new Weights(weights)

  def apply(args: Seq[String]): Option[Weights] = {
    val argLength = args.length
    RequireClientProtocol(
      args != null && argLength > 0,
      "WEIGHTS can not be specified with an empty list")
    args.head.toUpperCase match {
      case WEIGHTS =>
        RequireClientProtocol(argLength > 1, "WEIGHTS requires additional arguments")
        val weights: Array[Double] = RequireClientProtocol.safe {
          args.tail.map { item => NumberFormat.toDouble(item) }(collection.breakOut)
        }
        Some(new Weights(weights))
      case _ => None
    }
  }
  override def toString = Weights.WEIGHTS
}

// Handles parsing and manipulation of AGGREGATE arguments
sealed abstract class Aggregate(val name: String) {
  override def toString = Aggregate.toString + " " + name.toUpperCase
  def toChannelBuffer = ChannelBuffers.wrappedBuffer(toChannelBuffers.toArray:_*)
  def toChannelBuffers = Seq(Aggregate.AGGREGATE_CB, StringToChannelBuffer(name.toUpperCase))
  def equals(str: String) = str.equals(name)
}
object Aggregate {
  val AGGREGATE = "AGGREGATE"
  val AGGREGATE_CB = StringToChannelBuffer(AGGREGATE)
  case object Sum extends Aggregate("SUM")
  case object Min extends Aggregate("MIN")
  case object Max extends Aggregate("MAX")
  override def toString = AGGREGATE

  def apply(args: Seq[String]): Option[Aggregate] = {
    val argLength = args.length
    RequireClientProtocol(
      args != null && argLength > 0,
      "AGGREGATE can not be specified with empty list")
    args.head.toUpperCase match {
      case AGGREGATE =>
        RequireClientProtocol(argLength == 2, "AGGREGATE requires a type (MIN, MAX, SUM)")
        args(1).toUpperCase match {
          case Aggregate.Sum.name => Some(Aggregate.Sum)
          case Aggregate.Max.name => Some(Aggregate.Max)
          case Aggregate.Min.name => Some(Aggregate.Min)
          case _ => throw new ClientError("AGGREGATE type must be one of MIN, MAX or SUM")
        }
      case _ => None
    }
  }
}


object Count {
  val COUNT = "COUNT"
  val COUNT_CB = StringToChannelBuffer(COUNT)

  def apply(args: Seq[String]): Option[JLong] = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "COUNT can not be specified with empty list")
    args.head.toUpperCase match {
      case COUNT =>
        RequireClientProtocol(args.length == 2, "COUNT requires two arguments")
        Some(RequireClientProtocol.safe { NumberFormat.toLong(args(1)) })
      case _ => None
    }
  }
}


object Pattern {
  val PATTERN = "PATTERN"
  val PATTERN_CB = StringToChannelBuffer(PATTERN)

  def apply(args: Seq[String]): Option[String] = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "AGGREGATE can not be specified with empty list")
    args.head.toUpperCase match {
      case PATTERN => Some(args(1))
      case _ => None
    }
  }
}
