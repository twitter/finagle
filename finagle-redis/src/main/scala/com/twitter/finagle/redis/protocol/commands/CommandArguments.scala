package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.breakOut
import java.lang.{Long => JLong}

trait CommandArgument extends Command {
  override def toChannelBuffer: ChannelBuffer = BufChannelBuffer(encoded.reduce(_ concat _))

  private[redis] def encoded: Seq[Buf]
}

case object WithScores extends CommandArgument {
  def command = Commands.WITHSCORES

  def unapply(s: String) = s.toUpperCase match {
    case Commands.WITHSCORES => Some(s)
    case _ => None
  }

  override def encoded = Seq(Buf.Utf8(command))
  override def toString = command
}

case class Limit(offset: Long, count: Long) extends CommandArgument {
  def command = Commands.LIMIT

  override private[redis] def encoded = Seq(
    CommandBytes.LIMIT,
    Buf.Utf8(offset.toString),
    Buf.Utf8(count.toString)
  )

  override def toString = "%s %d %d".format(command, offset, count)
}

object Limit {
  def apply(args: Seq[String]) = {
    RequireClientProtocol(args != null && args.length == 3, "LIMIT requires two arguments")
    RequireClientProtocol(args.head == Commands.LIMIT, "LIMIT must start with LIMIT clause")
    RequireClientProtocol.safe {
      val offset = NumberFormat.toLong(args(1))
      val count = NumberFormat.toLong(args(2))
      new Limit(offset, count)
    }
  }
}

// Represents a list of WEIGHTS
class Weights(underlying: Array[Double]) extends CommandArgument with IndexedSeq[Double] {
  def command = Commands.WEIGHTS

  def apply(idx: Int) = underlying(idx)
  def length = underlying.length

  override private[redis] def encoded: Seq[Buf] =
    CommandBytes.WEIGHTS +: underlying.map(w => Buf.Utf8(w.toString))(breakOut)

  override def toString = Weights.toString + " " + this.mkString(" ")

}

// Handles parsing and manipulation of WEIGHTS arguments
object Weights {
  def apply(weight: Double) = new Weights(Array(weight))
  def apply(weights: Double*) = new Weights(weights.toArray)
  def apply(weights: Array[Double]) = new Weights(weights)

  def apply(args: Seq[String]): Option[Weights] = {
    val argLength = args.length
    RequireClientProtocol(
      args != null && argLength > 0,
      "WEIGHTS can not be specified with an empty list")
    args.head.toUpperCase match {
      case Commands.WEIGHTS =>
        RequireClientProtocol(argLength > 1, "WEIGHTS requires additional arguments")
        val weights: Array[Double] = RequireClientProtocol.safe {
          args.tail.map { item => NumberFormat.toDouble(item) }(collection.breakOut)
        }
        Some(new Weights(weights))
      case _ => None
    }
  }
  override def toString = Commands.WEIGHTS
}

// Handles parsing and manipulation of AGGREGATE arguments
sealed abstract class Aggregate(val name: String) extends CommandArgument {

  def command: String = s"${Commands.AGGREGATE} ${name.toUpperCase}"

  override private[redis] def encoded: Seq[Buf] =
    Seq(CommandBytes.AGGREGATE, Buf.Utf8(name.toUpperCase))

  def equals(str: String) = str.equals(name)
  override def toString = command
}

object Aggregate {
  case object Sum extends Aggregate("SUM")
  case object Min extends Aggregate("MIN")
  case object Max extends Aggregate("MAX")
  override def toString = Commands.AGGREGATE

  def apply(args: Seq[String]): Option[Aggregate] = {
    val argLength = args.length
    RequireClientProtocol(
      args != null && argLength > 0,
      "AGGREGATE can not be specified with empty list")
    args.head.toUpperCase match {
      case Commands.AGGREGATE =>
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

  def apply(args: Seq[String]): Option[JLong] = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "COUNT can not be specified with empty list")
    args.head.toUpperCase match {
      case Commands.AGGREGATE =>
        RequireClientProtocol(args.length == 2, "COUNT requires two arguments")
        Some(RequireClientProtocol.safe { NumberFormat.toLong(args(1)) })
      case _ => None
    }
  }
}


object Pattern {
  def apply(args: Seq[String]): Option[String] = {
    RequireClientProtocol(
      args != null && !args.isEmpty,
      "AGGREGATE can not be specified with empty list")
    args.head.toUpperCase match {
      case Commands.AGGREGATE => Some(args(1))
      case _ => None
    }
  }
}
