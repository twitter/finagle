package com.twitter.finagle.util

import scala.annotation.tailrec

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._

trait Statistic {
  // Future:  def sumOfSquares: Int
  def sum: Int
  def count: Int

  def average: Int = if (count != 0) sum / count else 0

  def add(value: Int): Unit = add(value, 1)
  def add(value: Int, count: Int)
}

class ScalarStatistic extends Statistic {
  private val serializer = new Serialized
  @volatile private var counter = 0
  @volatile private var accumulator = 0

  def sum = accumulator
  def count = counter

  def add(value: Int, count: Int) = serializer {
    counter += count
    accumulator += value
  }

  override def toString = "(Count: %s, Sum: %s)".format(count, sum)
}

class TimeWindowedStatistic[S <: Statistic](bucketCount: Int, bucketDuration: Duration)
  extends Statistic
{
  val collection = new TimeWindowedCollection[ScalarStatistic](bucketCount, bucketDuration)

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
