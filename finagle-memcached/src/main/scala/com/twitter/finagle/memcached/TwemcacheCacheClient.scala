package com.twitter.finagle.memcached

import _root_.java.lang.{Boolean => JBoolean}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.Stored
import com.twitter.finagle.memcached.protocol.Exists
import com.twitter.finagle.Service
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}

// Client interface supporting twemcache commands
trait TwemcacheClient extends Client {
  /**
   * Get a set of keys from the server, together with a "version"
   * token.
   *
   * @return a Map[String, (Buf, Buf)] of all the
   * keys the server had, together with their "version" token
   */
  def getv(key: String): Future[Option[(Buf, Buf)]] =
    getv(Seq(key)).map { _.values.headOption }

  def getv(keys: Iterable[String]): Future[Map[String, (Buf, Buf)]] = {
    getvResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithTokens)
      }
    }
  }

  /**
   * Get the server returned value for a set of keys for getv operation, in the format
   * of GetsResult since the data format for gets and getv are identical
   * @return
   */
  def getvResult(keys: Iterable[String]): Future[GetsResult]

  /**
   * Perform a UPSERT operation on the key, only if the value 'version' is not
   * newer than the provided one.
   *
   * @return true if replaced, false if not
   */
  def upsert(key: String, value: Buf, version: Buf): Future[JBoolean] =
    upsert(key, 0, Time.epoch, value, version)

  def upsert(key: String, flags: Int, expiry: Time, value: Buf, version: Buf): Future[JBoolean]
}

/**
 * Twemcache commands implementation.
 * This trait can only be mixed into a memcache client implementation as extension.
 */
trait TwemcacheConnectedClient extends TwemcacheClient { self: ConnectedClient =>
  def getvResult(keys: Iterable[String]): Future[GetsResult] = {
    try {
      if (keys==null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      val bufs =  keys.map { Buf.Utf8(_) }.toSeq
      rawGet(Getv(bufs)).map { GetsResult(_) } // map to GetsResult as the response format are the same
    }  catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def upsert(key: String, flags: Int, expiry: Time, value: Buf, version: Buf): Future[JBoolean] = {
    try {
      service(Upsert(Buf.Utf8(key), flags, expiry, value, version)).map {
        case Stored() => true
        case Exists() => false
        case Error(e) => throw e
        case _        => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }
}

object TwemcacheClient {
  /**
   * Construct a twemcache client from a single Service, which supports both memcache and twemcache command
   */
  def apply(raw: Service[Command, Response]): TwemcacheClient = {
    new ConnectedClient(raw) with TwemcacheConnectedClient
  }
}

/**
 * Twemcache commands implemenation for a partitioned client.
 * This trait can only be mixed into a ParitionedClient that is delegating twemcache compatible clients.
 */
trait TwemcachePartitionedClient extends TwemcacheClient { self: PartitionedClient =>

  // For now we requires the ParitionedClient must be delgating TwemcacheClient.
  // Refactory is on the way to re-archytect the partitioned client
  protected[memcached] def twemcacheClientOf(key: String): TwemcacheClient = clientOf(key).asInstanceOf[TwemcacheClient]

  def getvResult(keys: Iterable[String]) = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
        _.getvResult(_)
      }.map { GetResult.merged(_) }
    } else {
      Future.value(GetsResult(GetResult()))
    }
  }

  def upsert(key: String, flags: Int, expiry: Time, value: Buf, version: Buf) =
    twemcacheClientOf(key).upsert(key, flags, expiry, value, version)

  private[this] def withKeysGroupedByClient[A](
      keys: Iterable[String])(f: (TwemcacheClient, Iterable[String]) => Future[A]
      ): Future[Seq[A]] = {
    Future.collect(
      keys.groupBy(twemcacheClientOf).map(Function.tupled(f)).toSeq
    )
  }
}
