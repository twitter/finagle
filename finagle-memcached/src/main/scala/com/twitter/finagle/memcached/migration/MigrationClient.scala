package com.twitter.finagle.memcached.migration

import com.twitter.finagle.memcached._
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}

/**
 * DarkRead client.
 * requires backendClient;
 * override Get and Gets to send dark read to backend pool;
 * backend read results are not blocking or exposed.
 */
trait DarkRead extends Client {
  protected val backendClient: Client

  abstract override def getResult(keys: Iterable[String]) = {
    val frontResult = super.getResult(keys)
    val backResult = backendClient.getResult(keys)

    chooseGetResult(frontResult, backResult)
  }

  // DarkRead always choose the front result and ignore the backend one
  protected def chooseGetResult(frontResult: Future[GetResult], backResult: Future[GetResult]): Future[GetResult] = {
    frontResult
  }

  abstract override def getsResult(keys: Iterable[String]) = {
    val frontResult = super.getsResult(keys)
    val backResult = backendClient.getsResult(keys)

    chooseGetsResult(frontResult, backResult)
  }

  // DarkRead always choose the front result and ignore the backend one
  protected def chooseGetsResult(frontResult: Future[GetsResult], backResult: Future[GetsResult]): Future[GetsResult] = {
    frontResult
  }

  abstract override def release() {
    super.release()
    backendClient.release()
  }
}

/**
 * DarkWrite client.
 * requires backendClient;
 * override all write operation (except cas) to send dark write to backend pool;
 * backend write results are not blocking or exposed.
 */
trait DarkWrite extends Client {
  protected val backendClient: Client

  abstract override def set(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.set(key, flags, expiry, value)
    backendClient.set(key, flags, expiry, value)
    result
  }

  abstract override def add(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.add(key, flags, expiry, value)
    backendClient.add(key, flags, expiry, value)
    result
  }

  abstract override def append(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.append(key, flags, expiry, value)
    backendClient.append(key, flags, expiry, value)
    result
  }

  abstract override def prepend(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.prepend(key, flags, expiry, value)
    backendClient.prepend(key, flags, expiry, value)
    result
  }

  abstract override def replace(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.replace(key, flags, expiry, value)
    backendClient.replace(key, flags, expiry, value)
    result
  }

  abstract override def incr(key: String, delta: Long) = {
    val result = super.incr(key, delta)
    backendClient.incr(key, delta)
    result
  }

  abstract override def decr(key: String, delta: Long) = {
    val result = super.decr(key, delta)
    backendClient.decr(key, delta)
    result
  }

  // cas operation does not migrate
  abstract override def checkAndSet(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    super.checkAndSet(key, flags, expiry, value, casUnique)

  abstract override def delete(key: String) = {
    val result = super.delete(key)
    backendClient.delete(key)
    result
  }

  abstract override def release() {
    super.release()
    backendClient.release()
  }
}
