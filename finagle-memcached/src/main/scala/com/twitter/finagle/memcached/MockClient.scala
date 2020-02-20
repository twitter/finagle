package com.twitter.finagle.memcached

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.protocol.{ClientError, Value}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import scala.collection.mutable

object MockClient {
  case class Cached(value: Buf, expiry: Time = Time.epoch, flags: Int = 0)

  object Cached {
    val Empty = Cached(Buf.Empty)
  }

  def isExpired(exp: Time): Boolean =
    if (exp == Time.epoch) false else Time.now.floor(1.second) > exp.floor(1.second)

  private[memcached] def asValue(key: String, v: Buf, flags: Int = 0): Value =
    Value(
      Buf.Utf8(key),
      v,
      Some(Interpreter.generateCasUnique(v)),
      Some(Buf.Utf8(flags.toString))
    )

  private[memcached] def asValue(key: String, v: String, flags: Int): Value =
    asValue(key, Buf.Utf8(v), flags)

  def apply(xs: (String, MockClient.Cached)*): MockClient = {
    val mc = new MockClient()
    mc ++= xs
  }

  def fromStrings(xs: (String, String)*): MockClient =
    apply(xs.map { case (k, v) => k -> Cached(Buf.Utf8(v)) }: _*)

  /**
   * create a MockClient that ignores all TTL values for backwards compatibility
   * with code that expects the old MockClient behavior.
   */
  def ignoresTtl(): MockClient = new ExpiryIgnoringMockClient
}

/**
 * Map-based mock client for testing
 *
 * @note this class now respects expiry times. If you want the old expiry-ignoring behavior,
 *       use MockClient.ignoresTtl()
 */
class MockClient extends Client {
  import MockClient.Cached
  private[this] val mm = mutable.Map[String, MockClient.Cached]()

  protected def isExpired(t: Time): Boolean = MockClient.isExpired(t)

  protected def _get(keys: Iterable[String]): GetResult = {
    val hits = mutable.Map[String, Value]()
    val misses = mutable.Set[String]()

    mm.synchronized {
      keys.foreach { key =>
        mm.get(key) match {
          case Some(Cached(v, expiry, flags)) if !isExpired(expiry) =>
            hits += (key -> MockClient.asValue(key, v, flags))
          case _ =>
            misses += key
        }
        // Needed due to compiler bug: https://github.com/scala/bug/issues/10151
        ()
      }
    }
    GetResult(hits.toMap, misses.toSet)
  }

  /** convenience method for adding a value directly to the underlying map */
  def +=(kv: (String, Cached)): MockClient = {
    mm.synchronized { mm += kv }
    this
  }

  /** convenience method for adding several values directly to the underlying map */
  def ++=(kvs: TraversableOnce[(String, Cached)]): MockClient = {
    mm.synchronized { mm ++= kvs }
    this
  }

  def update(k: String, v: Cached): Unit = this += (k -> v)
  def update(k: String, v: String): Unit = update(k, Buf.Utf8(v))
  def update(k: String, v: Buf): Unit = this += k -> Cached(v)

  def getResult(keys: Iterable[String]): Future[GetResult] =
    Future.value(_get(keys))

  def getsResult(keys: Iterable[String]): Future[GetsResult] =
    Future.value(GetsResult(_get(keys)))

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Unit] = {
    mm.synchronized {
      mm += (key -> Cached(value, expiry, flags))
    }
    Future.Unit
  }

  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      mm.synchronized {
        val f: () => Boolean = () => {
          mm += (key -> Cached(value, expiry, flags))
          true
        }

        mm.get(key) match {
          case None => f()
          case Some(Cached(_, exp, _)) if isExpired(exp) => f()
          case _ => false
        }
      }
    )

  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case None => false
          case Some(Cached(_, exp, _)) if isExpired(exp) => false
          case Some(Cached(buf, _, _)) =>
            mm += (key -> Cached(buf.concat(value), expiry, flags))
            true
        }
      }
    )

  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case None => false
          case Some(Cached(_, exp, _)) if isExpired(exp) => false

          case Some(Cached(buf, _, _)) =>
            mm += (key -> Cached(value.concat(buf), expiry, flags))
            true
        }
      }
    )

  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case None => false
          case Some(Cached(_, exp, _)) if isExpired(exp) => false
          case _ =>
            mm += (key -> Cached(value, expiry, flags))
            true
        }
      }
    )

  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: Buf,
    casUnique: Buf
  ): Future[CasResult] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case None => CasResult.NotFound
          case Some(Cached(_, exp, _)) if isExpired(exp) => CasResult.NotFound
          case Some(Cached(buf, _, _)) if Interpreter.generateCasUnique(buf) == casUnique =>
            mm += (key -> Cached(value, expiry, flags))
            CasResult.Stored
          case _ =>
            CasResult.Exists
        }
      }
    )

  def delete(key: String): Future[JBoolean] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case Some(Cached(_, exp, _)) =>
            mm.remove(key)
            !isExpired(exp)
          case None =>
            false
        }
      }
    )

  def incr(key: String, delta: Long): Future[Option[JLong]] =
    Future.value(
      mm.synchronized {
        mm.get(key) match {
          case Some(cached @ Cached(value, exp, _)) if !isExpired(exp) =>
            try {
              val Buf.Utf8(valStr) = value
              val newValue = math.max(valStr.toLong + delta, 0L)
              mm(key) = cached.copy(value = Buf.Utf8(newValue.toString))
              Some(newValue)
            } catch {
              case _: NumberFormatException =>
                throw new ClientError("cannot increment or decrement non-numeric value")
            }
          case _ => None
        }
      }
    )

  def decr(key: String, delta: Long): Future[Option[JLong]] =
    incr(key, -delta)

  def stats(args: Option[String]): Future[Seq[String]] = Future.Nil

  def close(deadline: Time): Future[Unit] = Future.Done

  /** clear underlying cache: the underlying Map will be empty */
  def clear(): Unit = mm.synchronized { mm.clear() }

  def size: Int = mm.synchronized { mm.size }

  /**
   * Returns an immutable copy of the current cache. API compatible with MockClient
   *
   * @note The returned Map will contain expired items for backwards compatibility
   *       with the old API. To get a Map with them filtered out, use MockClient.active
   */
  def contents: Map[String, Buf] =
    toMap.map { case (k, Cached(buf, _, _)) => k -> buf }

  /** Returns an immutable copy of the current cache with expired elements filtered out */
  def active: Map[String, Cached] =
    toMap.collect {
      case (k, v @ Cached(_, expiry, _)) if !isExpired(expiry) => k -> v
    }

  /**
   * Returns an immutable copy of the current cache. Values are wrapped in a Cached
   * class, which carries the expiry time and flags given, along with the Buf containing
   * the value.
   *
   * @note The returned Map *will* contain expired items. To get a Map with them filtered out,
   *       use MockClient.active
   */
  def toMap: Map[String, Cached] = mm.synchronized { Map(mm.toSeq: _*) }

  override def toString: String = mm.synchronized { s"MockClient($mm)" }

  def isEmpty: Boolean = mm.synchronized { mm.isEmpty }
}

/**
 * A testing client that never expires cache entries
 */
private class ExpiryIgnoringMockClient extends MockClient {
  override def isExpired(t: Time): Boolean = false
}
