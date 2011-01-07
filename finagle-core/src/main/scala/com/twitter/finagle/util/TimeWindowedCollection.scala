package com.twitter.finagle.util

import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._

class TimeWindowedCollection[A](bucketCount: Int, bucketDuration: Duration)
  (implicit val _a: Manifest[A])
  extends Iterable[A]
{
  // PREMATURE_OPTIMIZATION_TODO:
  //
  //   - we can keep the head reference in an atomic ref, and only
  //   ever synchronize on gc()
  //   - we can keep a (circular) array of counters instead of
  //   creating new objects every expiration.
  //   - use serialized!

  protected def newInstance: A =
    _a.erasure.newInstance.asInstanceOf[A]

  @volatile private var buckets = List[Tuple2[Time, A]]()

  private def gc(now: Time) = synchronized {
    val limit = now - bucketDuration * bucketCount

    val index = buckets.findIndexOf {
      case (timestamp, _) if timestamp < limit => true
      case _ => false
    }

    if (index != -1)
      buckets = buckets.slice(0, index)
  }

  private[util] def prepend(now: Time) = synchronized {
    gc(now)

    val instance = newInstance
    buckets = (now, instance) :: buckets
    instance
  }

  def apply(): A = synchronized {
    val now = Time.now

    buckets.headOption match  {
      case Some((timestamp, _)) if now - timestamp >= bucketDuration =>
        prepend(now)
      case Some((_, instance)) =>
        instance
      case None =>
        prepend(now)
    }
  }

  override def toString = synchronized {
    val details = buckets.headOption map { case (firstTs, _) =>
      val bucketDetails = buckets map {
        case (ts, instance) => "%s = %s".format(ts - firstTs, instance)
      }
      bucketDetails.mkString("\n\t", ",\n\t", "")
    }

    "TimeWindowedCollection[%s](%d buckets of %s sec, spanning %s) = [%s]".format(
      _a.erasure.getName, bucketCount, bucketDuration, details getOrElse "", timeSpan)
  }

  def timeSpan = {
    val now = Time.now
    (buckets.lastOption map { case (ts, _) => ts } getOrElse(now), now + bucketDuration)
  }

  // TODO: check that this is threadsafe. i believe it is (because we
  // get the head).
  def iterator = {
    gc(Time.now)
    buckets.iterator map { case (_, x) => x }
  }
}
