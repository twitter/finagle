package com.twitter.finagle.memcached

import com.twitter.finagle.memcached.protocol.{ClientError, Value}
import com.twitter.finagle.memcached.util.ChannelBufferUtils
import com.twitter.util.{Future, Time}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import scala.collection.mutable
import _root_.java.lang.{Boolean => JBoolean, Long => JLong}

/**
 * Map-based mock client for testing
 * Note: The cas method checks if value is same as previous value, if not, do a swap and return true
 */
class MockClient(val map: mutable.Map[String, ChannelBuffer]) extends Client {
  def this() = this(mutable.Map[String, ChannelBuffer]())

  def this(contents: Map[String, Array[Byte]]) =
    this(mutable.Map[String, ChannelBuffer]() ++ (contents mapValues { ChannelBuffers.wrappedBuffer(_) }))

  def this(contents: Map[String, String])(implicit m: Manifest[String]) =
    this(contents mapValues { _.getBytes })

  protected def _get(keys: Iterable[String]): GetResult = {
    val hits = mutable.Map[String, Value]()
    val misses = mutable.Set[String]()

    map.synchronized {
      keys foreach { key =>
        map.get(key) match {
          case Some(v: ChannelBuffer) =>
            hits += (key -> Value(ChannelBuffers.wrappedBuffer(key.getBytes), v))
          case _ =>
            misses += key
        }
        // Needed due to compiler bug(?)
        Unit
      }
    }
    GetResult(hits.toMap, misses.toSet)
  }

  def getResult(keys: Iterable[String]): Future[GetResult] =
    Future.value(_get(keys))

  def getsResult(keys: Iterable[String]): Future[GetsResult] =
    Future.value(GetsResult(_get(keys)))

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    map.synchronized { map(key) = value }
    Future.Unit
  }

  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[JBoolean] =
    Future.value(
      map.synchronized {
        if (!map.contains(key)) {
          map(key) = value
          true
        } else {
          false
        }
      }
    )

  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[JBoolean] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(previousValue) =>
            map(key) = ChannelBuffers.copiedBuffer(previousValue, value)
            true
          case None =>
            false
        }
      }
    )

  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[JBoolean] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(previousValue) =>
            map(key) = ChannelBuffers.copiedBuffer(value, previousValue)
            true
          case None =>
            false
        }
      }
    )

  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[JBoolean] =
    Future.value(
      map.synchronized {
        if (map.contains(key)) {
          map(key) = value
          true
        } else {
          false
        }
      }
    )

  def cas(key: String, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer): Future[JBoolean] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(previousValue) if previousValue != value =>
            map(key) = value
            true
          case _ =>
            false
        }
      }
    )

  def delete(key: String): Future[JBoolean] =
    Future.value(
      map.synchronized {
        if (map.contains(key)) {
          map.remove(key)
          true
        } else {
          false
        }
      }
    )

  def incr(key: String, delta: Long): Future[Option[JLong]] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(value: ChannelBuffer) =>
            try {
              val newValue = math.max((ChannelBufferUtils.channelBufferToString(value).toLong + delta), 0L)
              map(key) = ChannelBuffers.wrappedBuffer(newValue.toString.getBytes)
              Some(newValue)
            } catch {
              case _: NumberFormatException =>
                throw new ClientError("cannot increment or decrement non-numeric value")
            }

          case None =>
            None
        }
      }
    )

  def decr(key: String, delta: Long): Future[Option[JLong]] =
    incr(key, -delta)

  def stats(args: Option[String]): Future[Seq[String]] = Future.Nil

  def release() {}

  override def toString = {
    "MockClient(" + map.toString + ")"
  }
}
