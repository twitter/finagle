package com.twitter.finagle.redis


import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.Service
import com.twitter.util.Future


object Client {

  /**
   * Construct a client from a single host.
   *
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(new Redis())
      .build())

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: Service[Command, Reply]): Client = new Client(raw)

}

/**
 * Connects to a single Redis host
 * @param service: Finagle service object built with the Redis codec
 */
class Client(service: Service[Command, Reply]) {

  /**
   * Appends value at the given key. If key doesn't exist,
   * behavior is similar to SET command
   * @params key, value
   * @return Length of string after append operation
   */
  def append(key: String, value: Array[Byte]): Future[Int] =
    service(Append(key, value)) flatMap {
      case IntegerReply(id)     => Future.value(id)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Decrements number stored at key by given amount. If key doesn't
   * exist, value is set to 0 before the operation
   * @params key, amount
   * @return Value after decrement. Error if key contains value
   * of the wrong type
   */
  def decrBy(key: String, amount: Int): Future[Int] =
    service(DecrBy(key, amount)) flatMap {
      case IntegerReply(id)     => Future.value(id)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Gets the value associated with the given key
   * @param key
   * @return Option containing either the value byte array, or nothing
   * if key doesn't exist
   */
  def get(key: String): Future[Option[Array[Byte]]] =
    service(Get(key)) flatMap {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Gets the substring of the value associated with given key
   * @params key, start, end
   * @return Option containing the substring, or nothing if key doesn't exist
   */
  def getRange(key: String, start: Int, end: Int): Future[Option[Array[Byte]]] =
    service(GetRange(key, start, end)) flatMap {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Sets the given value to key. If a value already exists for the key,
   * the value is overwritten with the new value
   * @params key, value
   */
  def set(key: String, value: Array[Byte]): Future[Unit] =
    service(Set(key, value)) flatMap {
      case StatusReply(message) => Future.value(Unit)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Removes keys
   * @param list of keys to remove
   * @return Number of keys removed
   */
  def del(keys: Seq[String]): Future[Int] =
    service(Del(keys.toList)) flatMap {
      case IntegerReply(id)     => Future.value(id)
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Checks if given key exists
   * @param key
   * @return True if key exists, false otherwise
   */
  def exists(key: String): Future[Boolean] =
    service(Exists(key)) flatMap {
      case IntegerReply(id)     => Future.value((id == 1))
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    }

  /**
   * Releases underlying service object
   */
  def release() = service.release()

}