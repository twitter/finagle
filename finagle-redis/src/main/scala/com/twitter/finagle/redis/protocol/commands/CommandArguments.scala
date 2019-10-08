package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import scala.collection.compat.immutable.ArraySeq

trait CommandArgument extends Command

case object WithScores extends CommandArgument {
  def name: Buf = Command.WITHSCORES
}

case class Limit(offset: Long, count: Long) extends CommandArgument {
  def name: Buf = Command.LIMIT
  override def body: Seq[Buf] = Seq(Buf.Utf8(offset.toString), Buf.Utf8(count.toString))
}

// Represents a list of WEIGHTS
class Weights(underlying: Array[Double]) extends CommandArgument with IndexedSeq[Double] {
  def name: Buf = Command.WEIGHTS

  def apply(idx: Int) = underlying(idx)
  def length = underlying.length

  override def body: Seq[Buf] = ArraySeq.unsafeWrapArray(underlying.map(w => Buf.Utf8(w.toString)))

  override def toString: String = Weights.toString + " " + this.mkString(" ")
}

// Handles parsing and manipulation of WEIGHTS arguments
object Weights {
  def apply(weight: Double) = new Weights(Array(weight))
  def apply(weights: Double*) = new Weights(weights.toArray)
  def apply(weights: Array[Double]) = new Weights(weights)
}

// Handles parsing and manipulation of AGGREGATE arguments
sealed abstract class Aggregate(val sub: String) extends CommandArgument {
  def name: Buf = Command.AGGREGATE
  override def body: Seq[Buf] = Seq(Buf.Utf8(sub.toUpperCase))
  def equals(str: String): Boolean = str.equals(sub)
}

object Aggregate {
  case object Sum extends Aggregate("SUM")
  case object Min extends Aggregate("MIN")
  case object Max extends Aggregate("MAX")
}
