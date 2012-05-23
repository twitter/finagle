package com.twitter.finagle.redis
package protocol

import util._

trait CommandArgument extends Command {
  override def toChannelBuffer =
    throw new UnsupportedOperationException("OptionCommand does not support toChannelBuffer")
}

// Constant case object representing WITHSCORES command arg
case object WithScores extends CommandArgument {
  val WITHSCORES = "WITHSCORES"
  val command = WITHSCORES
  override def toString = WITHSCORES
  def unapply(s: String) = s.toUpperCase match {
    case WITHSCORES => Some(s)
    case _ => None
  }
  val asArg = Some(WithScores)
}

case class Limit(offset: Int, count: Int) extends CommandArgument {
  override def toString = "%s %d %d".format(Limit.LIMIT, offset, count)
  val command = Limit.LIMIT
}
object Limit {
  val LIMIT = "LIMIT"
  def apply(args: List[String]) = {
    RequireClientProtocol(args != null && args.length == 3, "LIMIT requires two arguments")
    RequireClientProtocol(args.head == LIMIT, "LIMIT must start with LIMIT clause")
    RequireClientProtocol.safe {
      val offset = NumberFormat.toInt(args(1))
      val count = NumberFormat.toInt(args(2))
      new Limit(offset, count)
    }
  }
}

// Represents a list of WEIGHTS
class Weights(underlying: Vector[Double]) extends CommandArgument with IndexedSeq[Double] {
  override def apply(idx: Int) = underlying(idx)
  override def length = underlying.length
  override def toString = Weights.toString + " " + this.mkString(" ")
  val command = Weights.WEIGHTS
}

// Handles parsing and manipulation of WEIGHTS arguments
object Weights {
  val WEIGHTS = "WEIGHTS"

  def apply(weight: Double) = new Weights(Vector(weight))
  def apply(weights: Double*) = new Weights(Vector(weights:_*))
  def apply(weights: Vector[Double]) = new Weights(weights)

  def apply(args: List[String]): Option[Weights] = {
    val argLength = args.length
    RequireClientProtocol(
      args != null && argLength > 0,
      "WEIGHTS can not be specified with an empty list")
    args.head.toUpperCase match {
      case WEIGHTS =>
        RequireClientProtocol(argLength > 1, "WEIGHTS requires additional arguments")
        val weights: Vector[Double] = RequireClientProtocol.safe {
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
  def equals(str: String) = str.equals(name)
}
object Aggregate {
  val AGGREGATE = "AGGREGATE"
  case object Sum extends Aggregate("SUM")
  case object Min extends Aggregate("MIN")
  case object Max extends Aggregate("MAX")
  override def toString = AGGREGATE

  def apply(args: List[String]): Option[Aggregate] = {
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
