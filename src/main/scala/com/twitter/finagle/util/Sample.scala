package com.twitter.finagle.util

import scala.annotation.tailrec

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._

// TODO: do we want a decaying stat?

trait Sample {
  // TODO:  sumOfSquares
  def sum: Int
  def count: Int
  def mean: Int = if (count != 0) sum / count else 0

  override def toString = "[count=%d, sum=%d, mean=%d]".format(count, sum, mean)
}

trait AddableSample extends Sample {
  def add(value: Int): Unit = add(value, 1)
  def add(value: Int, count: Int)
  def incr(): Unit = add(0, 1)
}

class ScalarSample extends AddableSample with Serialized {
  @volatile private var counter = 0
  @volatile private var accumulator = 0

  def sum = accumulator
  def count = counter

  def add(value: Int, count: Int) = serialized {
    counter += count
    accumulator += value
  }
}

trait AggregateSample extends Sample {
  protected val underlying: Iterable[Sample]

  def sum   = underlying.map(_.sum).sum
  def count = underlying.map(_.count).sum
}

class TimeWindowedSample[S <: AddableSample](bucketCount: Int, bucketDuration: Duration)
  (implicit val _s: Manifest[S])
  extends AggregateSample with AddableSample
{
  protected val underlying = new TimeWindowedCollection[S](bucketCount, bucketDuration)

  def add(count: Int, value: Int) = underlying().add(count, value)

  def rateInHz = {
    val (begin, end) = underlying.timeSpan
    val timeDiff = end - begin
    count / timeDiff.inSeconds
  }

  // TODO: export for duration (to get different granularities but snaps to next bucket)
  //   def apply(duration: Duration): AggregateSample

  override def toString = underlying.toString
}

// TODO: aggregate per *LEAF name*

sealed abstract class SampleTree extends AggregateSample

case class SampleNode(name: String, underlying: Seq[SampleTree])
  extends SampleTree
{
  override def toString = {
    val lines = underlying flatMap (_.toString.split("\n")) map ("_" + _) mkString "\n"
    "%s %s".format(name, super.toString) + "\n" + lines
  }
}

case class SampleLeaf(name: String, sample: Sample)
  extends SampleTree
{
  val underlying = Seq(sample)
  override def toString = "%s %s".format(name, super.toString)
}
