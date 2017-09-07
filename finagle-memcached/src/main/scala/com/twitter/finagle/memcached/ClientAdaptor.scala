package com.twitter.finagle.memcached

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.bijection.Bijection
import com.twitter.io.Buf
import com.twitter.util.{Time, Future}

class ClientAdaptor[T](val self: Client, bijection: Bijection[Buf, T])
    extends BaseClient[T]
    with Proxy {
  def bufferToType(a: Buf): T = bijection(a)

  def set(key: String, flags: Int, expiry: Time, value: T): Future[Unit] =
    self.set(key, flags, expiry, bijection.inverse(value))
  def add(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean] =
    self.add(key, flags, expiry, bijection.inverse(value))
  def append(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean] =
    self.append(key, flags, expiry, bijection.inverse(value))
  def prepend(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean] =
    self.prepend(key, flags, expiry, bijection.inverse(value))
  def replace(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean] =
    self.replace(key, flags, expiry, bijection.inverse(value))
  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: T,
    casUnique: Buf
  ): Future[CasResult] =
    self.checkAndSet(key, flags, expiry, bijection.inverse(value), casUnique)

  def getResult(keys: Iterable[String]): Future[GetResult] = self.getResult(keys)
  def getsResult(keys: Iterable[String]): Future[GetsResult] = self.getsResult(keys)

  def delete(key: String): Future[JBoolean] = self.delete(key)
  def incr(key: String, delta: Long): Future[Option[JLong]] = self.incr(key, delta)
  def decr(key: String, delta: Long): Future[Option[JLong]] = self.decr(key, delta)

  def stats(args: Option[String]): Future[Seq[String]] = self.stats(args)

  def close(deadline: Time): Future[Unit] = self.close()
}
