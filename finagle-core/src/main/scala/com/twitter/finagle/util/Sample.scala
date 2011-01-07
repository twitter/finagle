package com.twitter.finagle.util

import scala.collection.mutable.Queue
import scala.collection.JavaConversions._

import java.util.concurrent.ConcurrentHashMap

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

trait AddableSample[+S <: AddableSample[S]] extends Sample {
  def add(value: Int): Unit = add(value, 1)
  def add(value: Int, count: Int)
  def incr(): Unit = add(0, 1)
}

trait SampleRepository[+S <: Sample] {
  def apply(path: String*): S
}

trait LazilyCreatingSampleRepository[S <: Sample] extends SampleRepository[S] {
  private val map = new ConcurrentHashMap[Seq[String], S]()

  def makeStat: S
  def apply(path: String*): S = map getOrElseUpdate(path, makeStat)
}

trait ObservableSampleRepository[S <: AddableSample[S]]
  extends LazilyCreatingSampleRepository[AddableSample[S]]
{
  private def tails[A](s: Seq[A]): Seq[Seq[A]] = {
    s match {
      case s@Seq(_) =>
        Seq(s)

      case Seq(hd, tl@_*) =>
        Seq(Seq(hd)) ++ (tails(tl) map { t => Seq(hd) ++ t })
    }
  }

  private type Observer = (Seq[String], Int, Int) => Unit
  private val observers = new Queue[Observer]
  private val tailObservers = new Queue[Observer]

  def observeTailsWith(o: (Seq[String], Int, Int) => Unit) = synchronized {
    tailObservers += o
  }

  def observeWith(o: (Seq[String], Int, Int) => Unit) = synchronized {
    observers += o
  }

  override def apply(path: String*): AddableSample[S] =
    new AddableSampleProxy[S](super.apply(path:_*)) {
      override def add(value: Int, count: Int) {
        super.add(value, count)

        ObservableSampleRepository.this.synchronized {
          for (o <- observers)
            o(path, value, count)

          for (tail <- tails(path); o <- tailObservers)
            o(path, value, count)
        }
      }
    }
}

class AddableSampleProxy[+S <: AddableSample[S]](
  val self: AddableSample[S])
  extends AddableSample[S] with Proxy
{
  def add(value: Int, count: Int) = self.add(value, count)
  def sum = self.sum
  def count = self.count
}

class ScalarSample extends AddableSample[ScalarSample] {
  @volatile private var counter = 0
  @volatile private var accumulator = 0

  def sum = accumulator
  def count = counter

  def add(value: Int, count: Int) = synchronized {
    counter += count
    accumulator += value
  }
}

trait AggregateSample extends Sample {
  protected val underlying: Iterable[Sample]

  def sum   = underlying.map(_.sum).sum
  def count = underlying.map(_.count).sum
}

class TimeWindowedSample[S <: AddableSample[S]](bucketCount: Int, bucketDuration: Duration)
  (implicit val _s: Manifest[S])
  extends AggregateSample with AddableSample[TimeWindowedSample[S]]
{
  protected val underlying = new TimeWindowedCollection[S](bucketCount, bucketDuration)

  def add(value: Int, count: Int) = underlying().add(value, count)

  def rateInHz = {
    val (begin, end) = underlying.timeSpan
    val timeDiff = end - begin
    count / timeDiff.inSeconds
  }

  // TODO: export for duration (to get different granularities but snaps to next bucket)
  //   def apply(duration: Duration): AggregateSample

  override def toString = underlying.toString
}

object TimeWindowedSample {
  def apply[S <: AddableSample[S]]
    (window: Duration, granularity: Duration)
    (implicit _s: Manifest[S]): TimeWindowedSample[S] =
  {
    if (window < granularity)
      throw new IllegalArgumentException("window smaller than granularity!")

    val numBuckets = math.max(1, window.inMilliseconds / granularity.inMilliseconds)
    new TimeWindowedSample[S](numBuckets.toInt, granularity)
  }

}

sealed abstract class SampleTree extends AggregateSample {
  val name: String
  def merge(other: SampleTree): SampleTree
}

case class SampleNode(name: String, underlying: Seq[SampleTree])
  extends SampleTree
{
  // In order to merge succesfully, trees must have the same shape.
  def merge(other: SampleTree) =
    other match {
      case SampleNode(otherName, otherUnderlying) if name == otherName =>
        val sampless =
          (underlying ++ otherUnderlying) groupBy (_.name) map { case (_, samples) => samples }
        val merged = sampless map { _.reduceLeft (_.merge(_)) }
        SampleNode(name, merged toSeq)

      case _ =>
        throw new IllegalArgumentException("trees are shape divergent")
    }

  override def toString = {
    val lines = underlying flatMap (_.toString.split("\n")) map ("_" + _) mkString "\n"
    "%s %s".format(name, super.toString) + "\n" + lines
  }
}

case class SampleLeaf(name: String, sample: Sample) extends SampleTree
{
  val underlying = Seq(sample)
  override def toString = "%s %s".format(name, super.toString)

  def merge(other: SampleTree) = {
    other match {
      case SampleLeaf(otherName, otherSample) if name == otherName =>
        SampleLeaf(name, new AggregateSample { val underlying = Seq(sample, otherSample) })

      // Shape divergence!
      case _ =>
        throw new IllegalArgumentException("trees are shape divergent")
    }
  }
}


