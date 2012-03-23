package com.twitter.finagle.redis

import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.BytesToString
import com.twitter.finagle.Service
import com.twitter.util.Future


object Client {

  /**
   * Construct a client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Redis())
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
    doRequest(Append(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Decrements number stored at key by given amount. If key doesn't
   * exist, value is set to 0 before the operation
   * @params key, amount
   * @return Value after decrement. Error if key contains value
   * of the wrong type
   */
  def decrBy(key: String, amount: Int): Future[Int] =
    doRequest(DecrBy(key, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value associated with the given key
   * @param key
   * @return Option containing either the value byte array, or nothing
   * if key doesn't exist
   */
  def get(key: String): Future[Option[Array[Byte]]] =
    doRequest(Get(key)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets the substring of the value associated with given key
   * @params key, start, end
   * @return Option containing the substring, or nothing if key doesn't exist
   */
  def getRange(key: String, start: Int, end: Int): Future[Option[Array[Byte]]] =
    doRequest(GetRange(key, start, end)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Sets the given value to key. If a value already exists for the key,
   * the value is overwritten with the new value
   * @params key, value
   */
  def set(key: String, value: Array[Byte]): Future[Unit] =
    doRequest(Set(key, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Removes keys
   * @param list of keys to remove
   * @return Number of keys removed
   */
  def del(keys: Seq[String]): Future[Int] =
    doRequest(Del(keys.toList)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Checks if given key exists
   * @param key
   * @return True if key exists, false otherwise
   */
  def exists(key: String): Future[Boolean] =
    doRequest(Exists(key)) {
      case IntegerReply(n) => Future.value((n == 1))
    }

  /**
   * Deletes all keys in current DB
   */
  def flushDB(): Future[Unit] =
    doRequest(FlushDB()) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Select DB with specified zero-based index
   * @param index
   * @return Status reply
   */
  def select(index: Int): Future[String] =
    doRequest(Select(index)) {
      case StatusReply(message) => Future.value(message)
    }

  /** Hashset Commands */

  /**
   * Deletes fields from given hash
   * @param hash key, fields
   * @return Number of fields deleted
   */
  def hDel(key: String, fields: Seq[String]): Future[Int] =
    doRequest(HDel(key, fields)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets field from hash
   * @param hash key, field
   * @return Value if field exists
   */
  def hGet(key: Array[Byte], field: Array[Byte]): Future[Option[Array[Byte]]] =
    doRequest(HGet(key, field)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets all field value pairs for given hash
   * @param hash key
   * @return List of field value pairs
   */
  def hGetAll(key: Array[Byte]): Future[Seq[Array[Byte]]] =
    doRequest(HGetAll(key)) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Gets values for given fields in hash
   * @param hash key, fields
   * @return List of values
   */
  def hMGet(key: String, fields: Seq[String]): Future[Seq[Array[Byte]]] =
    doRequest(HMGet(key, fields)) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Sets field value pair in given hash
   * @param hash key, field, value
   * @return 1 if field is new, 0 if field was updated
   */
  def hSet(key: Array[Byte], field: Array[Byte], value: Array[Byte]): Future[Int] =
    doRequest(HSet(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /** Sorted Set commands */

  /**
   * Adds member, score pair to sorted set
   * @params key, score, member
   * @return Number of elements added to sorted set
   */
  def zAdd(key: Array[Byte], score: Double, member: Array[Byte]): Future[Int] =
    doRequest(ZAdd(key, ZMember(score, member))) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets score of member in sorted set
   * @param key, member
   * @returns Score of member as a byte array
   */
  def zScore(key: Array[Byte], member: Array[Byte]): Future[Option[Array[Byte]]] =
    doRequest(ZScore(key, member)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets number of elements in sorted set with score between min and max
   * @params key, min, max
   * @return Number of elements between min and max in sorted set
   */
  def zCount(key: Array[Byte], min: Double, max: Double): Future[Int] =
    doRequest(ZCount(key, ZInterval(min), ZInterval(max))) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets member, score pairs from sorted set between min and max
   * Results are limited by offset and count
   * @param key, min, max, offset, count
   * @return Member, score pairs that match constraints
   */
  def zRangeByScoreWithScores(
    key: Array[Byte], min: Double, max: Double, offset: Int, count: Int
  ): Future[Seq[Array[Byte]]] =
    doRequest(
      ZRangeByScore(
        BytesToString(key),
        ZInterval(min),
        ZInterval(max),
        Some(WithScores),
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns sorted set cardinality of the sorted set at key
   * @param key
   * @return Integer representing cardinality of sorted set,
   * or 0 if key does not exist
   */
  def zCard(key: Array[Byte]): Future[Int] =
    doRequest(ZCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
    * Removes specified member(s) from sorted set at key
    * @params key, member(s)
    * @return Number of members removed from sorted set
    */
  def zRem(key: Array[Byte], members: Seq[Array[Byte]]): Future[Int] =
    doRequest(ZRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key
   * Elements are ordered from highest to lowest score
   * @param key, start, stop
   * @return List of element in specified range
   */
  def zRevRange(key: Array[Byte], start: Int, stop: Int): Future[Seq[Array[Byte]]] =
    doRequest(ZRevRange(key, start, stop)) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns elements in sorted set at key with a score between max and min
   * Elements are ordered from highest to lowest score
   * Results are limited by offset and count
   * @param key, max, min, offset, count
   * @return List of element in specified score range
   */
  def zRevRangeByScoreWithScores(
    key: Array[Byte], max: Double, min: Double, offset: Int, count: Int
  ): Future[Seq[Array[Byte]]] =
    doRequest(
      ZRevRangeByScore(
        BytesToString(key),
        ZInterval(max),
        ZInterval(min),
        Some(WithScores),
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Releases underlying service object
   */
  def release() = service.release()

  /**
   * Helper function for passing a command to the service
   */
  private def doRequest[T](cmd: Command)(handler: PartialFunction[Reply, Future[T]]) =
    service(cmd) flatMap (handler orElse {
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    })

}
