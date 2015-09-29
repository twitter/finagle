package com.twitter.finagle.memcached

import com.twitter.finagle.memcached.protocol.{ClientError, Value}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import scala.collection.mutable
import _root_.java.lang.{Boolean => JBoolean, Long => JLong}

/**
 * Map-based mock client for testing
 *
 * Note: expiry and flags are ignored on update operations.
 */
class MockClient(val map: mutable.Map[String, Buf]) extends Client {
  def this() = this(mutable.Map[String, Buf]())

  def this(contents: Map[String, Array[Byte]]) =
    this(mutable.Map[String, Buf]() ++ (contents mapValues { v => Buf.ByteArray(v) }))

  def this(contents: Map[String, String])(implicit m: Manifest[String]) =
    this(contents mapValues { _.getBytes })

  protected def _get(keys: Iterable[String]): GetResult = {
    val hits = mutable.Map[String, Value]()
    val misses = mutable.Set[String]()

    map.synchronized {
      keys foreach { key =>
        map.get(key) match {
          case Some(v: Buf) =>
            hits += (key -> Value(Buf.Utf8(key), v, Some(Interpreter.generateCasUnique(v))))
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

  /**
   * Note: expiry and flags are ignored.
   */
  def set(key: String, flags: Int, expiry: Time, value: Buf) = {
    map.synchronized { map(key) = value }
    Future.Unit
  }

  /**
   * Note: expiry and flags are ignored.
   */
  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
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

  /**
   * Note: expiry and flags are ignored.
   */
  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(previousValue) =>
            map(key) = previousValue.concat(value)
            true
          case None =>
            false
        }
      }
    )

  /**
   * Note: expiry and flags are ignored.
   */
  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      map.synchronized {
        map.get(key) match {
          case Some(previousValue) =>
            map(key) = value.concat(previousValue)
            true
          case None =>
            false
        }
      }
    )

  /**
   * Note: expiry and flags are ignored.
   */
  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
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

  /**
   * Checks if value is same as previous value, if not, do a swap and return true.
   *
   * Note: expiry and flags are ignored.
   */
  def cas(
    key: String,
    flags: Int,
    expiry: Time,
    value: Buf,
    casUnique: Buf
  ): Future[JBoolean] =
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
          case Some(value: Buf) =>
            try {
              val Buf.Utf8(valStr) = value
              val newValue = math.max(valStr.toLong + delta, 0L)
              map(key) = Buf.Utf8(newValue.toString)
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
