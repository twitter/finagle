package com.twitter.finagle.memcached

import com.twitter.util.{Time, Future, Bijection}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

class ClientAdaptor[T](
  val self: Client,
  bijection: Bijection[ChannelBuffer, T]
) extends BaseClient[T] with Proxy {
  def channelBufferToType(a: ChannelBuffer): T = bijection(a)

  def set(key: String, flags: Int, expiry: Time, value: T): Future[Unit] =
    self.set(key, flags, expiry, bijection.inverse(value))
  def add(key: String, flags: Int, expiry: Time, value: T): Future[Boolean] =
    self.add(key, flags, expiry, bijection.inverse(value))
  def append(key: String, flags: Int, expiry: Time, value: T): Future[Boolean] =
    self.append(key, flags, expiry, bijection.inverse(value))
  def prepend(key: String, flags: Int, expiry: Time, value: T): Future[Boolean] =
    self.prepend(key, flags, expiry, bijection.inverse(value))
  def replace(key: String, flags: Int, expiry: Time, value: T): Future[Boolean] =
    self.replace(key, flags, expiry, bijection.inverse(value))
  def cas(key: String, flags: Int, expiry: Time, value: T, casUnique: ChannelBuffer): Future[Boolean] =
    self.cas(key, flags, expiry, bijection.inverse(value), casUnique)

  def getResult(keys: Iterable[String]): Future[GetResult]   = self.getResult(keys)
  def getsResult(keys: Iterable[String]): Future[GetsResult] = self.getsResult(keys)

  def delete(key: String): Future[Boolean]                   = self.delete(key)
  def incr(key: String, delta: Long): Future[Option[Long]]   = self.incr(key, delta)
  def decr(key: String, delta: Long): Future[Option[Long]]   = self.decr(key, delta)

  def release(): Unit = self.release()
}
