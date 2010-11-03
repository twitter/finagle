package com.twitter.finagle.util

import scala.annotation.tailrec

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._

// TODO: do we want a decaying stat?

trait Statistic {
  // Future:  def sumOfSquares: Int
  def sum: Int
  def count: Int
  def mean: Int = if (count != 0) sum / count else 0
}

trait MutableStatistic extends Statistic {
  def add(value: Int): Unit = add(value, 1)
  def add(value: Int, count: Int)
  def incr(): Unit = add(0, 1)
}

class ScalarStatistic extends MutableStatistic with Serialized {
  @volatile private var counter = 0
  @volatile private var accumulator = 0

  def sum = accumulator
  def count = counter

  def add(value: Int, count: Int) = serialized {
    counter += count
    accumulator += value
  }

  override def toString = "(Count: %s, Sum: %s)".format(count, sum)
}

class TimeWindowedStatistic[S <: MutableStatistic](bucketCount: Int, bucketDuration: Duration)
  (implicit val _s: Manifest[S])
  extends MutableStatistic
{
  val collection = new TimeWindowedCollection[S](bucketCount, bucketDuration)
  def add(count: Int, value: Int) = collection().add(count, value)
  def sum = collection.map(_.sum).sum
  def count = collection.map(_.count).sum

  def rateInHz = {
    val (begin, end) = collection.timeSpan
    val timeDiff = end - begin
    count / timeDiff.inSeconds
  }

  override def toString = collection.toString
}

trait AggregateStatistic extends Statistic {
  val underlying: Seq[Statistic]
  def sum = underlying.map(_.sum).sum
  def count = underlying.map(_.count).sum
}

trait StatisticMap extends Map[String, Statistic]
