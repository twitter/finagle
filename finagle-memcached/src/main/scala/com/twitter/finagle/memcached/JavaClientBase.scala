package com.twitter.finagle.memcached

import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import scala.jdk.CollectionConverters._
import java.util.{List => juList, Map => juMap}

class JavaClientBase(underlying: Client) extends JavaClient {

  override def get(key: String): Future[Buf] = underlying.get(key).map(_.getOrElse(null))
  override def gets(key: String): Future[ResultWithCAS] =
    underlying
      .gets(key).map(_.map {
        case (b1, b2) => new ResultWithCAS(b1, b2)
      }.getOrElse(null))
  override def get(keys: juList[String]): Future[juMap[String, Buf]] =
    underlying.get(keys.asScala).map(_.asJava)
  override def gets(keys: juList[String]): Future[juMap[String, ResultWithCAS]] =
    underlying
      .gets(keys.asScala)
      .map(_.mapValues {
        case (b1, b2) => new ResultWithCAS(b1, b2)
      }.toMap.asJava)

  override def set(key: String, value: Buf): Future[Void] =
    underlying.set(key, value).map(_ => null)
  override def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Void] =
    underlying.set(key, flags, expiry, value).map(_ => null)
  override def add(key: String, value: Buf) = underlying.add(key, value)
  override def add(key: String, flags: Int, expiry: Time, value: Buf) =
    underlying.add(key, flags, expiry, value)
  override def append(key: String, value: Buf) = underlying.append(key, value)
  override def prepend(key: String, value: Buf) = underlying.prepend(key, value)
  override def replace(key: String, value: Buf) = underlying.replace(key, value)
  override def replace(key: String, flags: Int, expiry: Time, value: Buf) =
    underlying.replace(key, flags, expiry, value)

  override def cas(key: String, value: Buf, casUnique: Buf) =
    underlying.checkAndSet(key, value, casUnique).map(_.replaced)
  override def cas(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    underlying.checkAndSet(key, flags, expiry, value, casUnique).map(_.replaced)

  override def delete(key: String) = underlying.delete(key)
  override def incr(key: String) = underlying.incr(key).map(_.getOrElse(-1L))
  override def incr(key: String, delta: Long) = underlying.incr(key, delta).map(_.getOrElse(-1L))
  override def decr(key: String) = underlying.decr(key).map(_.getOrElse(-1L))
  override def decr(key: String, delta: Long) = underlying.decr(key, delta).map(_.getOrElse(-1L))
  override def release() = underlying.close()

  def getResult(keys: juList[String]): Future[GetResult] = underlying.getResult(keys.asScala)
  def getsResult(keys: juList[String]): Future[GetsResult] = underlying.getsResult(keys.asScala)

}
