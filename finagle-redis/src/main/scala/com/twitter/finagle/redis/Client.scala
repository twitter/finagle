package com.twitter.finagle.redis

import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat}
import com.twitter.finagle.Service
import com.twitter.util.Future
import scala.collection.{Set => CollectionSet}

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
   * Gets the ttl of the given key.
   * @param key
   * @return Option containing either the ttl in seconds if the key exists 
   * and has a timeout, or else nothing.
   */
  def ttl(key: String): Future[Option[Int]] =
    doRequest(Ttl(key)) {
      case IntegerReply(n) => {
        if (n != -1) {
          Future.value(Some(n))
        }
        else {
          Future.value(None)
        }
      }
    }

  /**
   * Sets how long it will take the key to expire
   * @params key, ttl
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expire(key: String, ttl: Int): Future[Boolean] =
    doRequest(Expire(key, ttl)) {
      case IntegerReply(n) => Future.value(n == 1)
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
   * Gets the length of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return the length of the list.  Unassigned keys are considered empty
   * lists, and return 0.
   */
  def lLen(key: String): Future[Int] =
    doRequest(LLen(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value of the element at the indexth position in the list.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, index
   * @return an option of the value of the element at the indexth position in the list.
   * Nothing if the index is out of range.
   */
  def lIndex(key: String, index: Int): Future[Option[Array[Byte]]] =
    doRequest(LIndex(key, index)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Inserts a value after another pivot value in the list.
   * If the key is a non-list element,
   * an exception will be thrown.
   * @params key, pivot, value
   * @return an option of the new length of the list, or nothing if the pivot is not found, or
   * the list is empty.
   */
  def lInsertAfter(key: String, pivot: Array[Byte], value: Array[Byte]): Future[Option[Int]] =
    doRequest(LInsert(key, "AFTER", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Inserts a value before another pivot value in the list.
   * If the key is a non-list element,
   * an exception will be thrown.
   * @params key, pivot, value
   * @return an option of the new length of the list, or nothing if the pivot is not found, or the
   * list is empty.
   */
  def lInsertBefore(key: String, pivot: Array[Byte], value: Array[Byte]): Future[Option[Int]] =
    doRequest(LInsert(key, "BEFORE", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Pops a value off the front of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return an option of the value of the popped element, or nothing if the list is empty.
   */
  def lPop(key: String): Future[Option[Array[Byte]]] =
    doRequest(LPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
   * Pushes a value onto the front of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, value
   * @return the length of the list
   */
  def lPush(key: String, value: List[Array[Byte]]): Future[Int] =
    doRequest(LPush(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes count elements matching value from the list.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, count: The sgn of count describes whether it will remove them from the
   * back or the front of the list.  If count is 0, it will remove all instances, value
   * @return the number of removed elements.
   */
  def lRem(key: String, count: Int, value: Array[Byte]): Future[Int] =
    doRequest(LRem(key, count, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Sets the indexth element to be value.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, index, value
   */
  def lSet(key: String, index: Int, value: Array[Byte]): Future[Unit] =
    doRequest(LSet(key, index, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Gets the values in the range supplied.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, start (inclusive), end (inclusive)
   * @return a list of the value
   */
  def lRange(key: String, start: Int, end: Int): Future[List[Array[Byte]]] =
    doRequest(LRange(key, start, end)) {
      case MBulkReply(message) => Future.value(message)
      case EmptyMBulkReply() => Future.value(List())
    }

  /**
   * Pops a value off the end of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return an option of the value of the popped element, or nothing if the list is empty.
   */
  def rPop(key: String): Future[Option[Array[Byte]]] =
    doRequest(RPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
   * Pushes a value onto the end of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @params key, value
   * @return the length of the list
   */
  def rPush(key: String, value: List[Array[Byte]]): Future[Int] =
    doRequest(RPush(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes all of the elements from the list except for those in the range.
   * @params key, start (inclusive), end (exclusive)
   */
  def lTrim(key: String, start: Int, end: Int): Future[Unit] =
    doRequest(LTrim(key, start, end)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Adds elements to the set, according to the set property.
   * Throws an exception if the key does not refer to a set.
   * @params key, members
   * @return the number of new members added to the set.
   */
  def sAdd(key: String, members: List[Array[Byte]]): Future[Int] =
    doRequest(SAdd(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the members of the set.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return a list of the members
   */
  def sMembers(key: String): Future[CollectionSet[Array[Byte]]] =
    doRequest(SMembers(key)) {
      case MBulkReply(list) => Future.value(list toSet)
      case EmptyMBulkReply() => Future.value(CollectionSet())
    }

  /**
   * Is the member in the set?
   * Throws an exception if the key does not refer to a set.
   * @params key, members
   * @return a boolean, true if it is in the set, false otherwise.  Unassigned
   * keys are considered empty sets.
   */
  def sIsMember(key: String, member: Array[Byte]): Future[Boolean] =
    doRequest(SIsMember(key, member)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * How many elements are in the set?
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return the number of elements in the set.  Unassigned keys are considered
   * empty sets.
   */
  def sCard(key: String): Future[Int] =
    doRequest(SCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes the element from the set if it is in the set.
   * Throws an exception if the key does not refer to a set.
   * @params key, member
   * @return an integer, the number of elements removed from the set, can be
   * 0 if the key is unassigned.
   */
  def sRem(key: String, members: List[Array[Byte]]): Future[Int] =
    doRequest(SRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes an element randomly from the set, and returns it.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return the member, or nothing if the set is empty.
   */
  def sPop(key: String): Future[Option[Array[Byte]]] =
    doRequest(SPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
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
   * Authorizes to db
   */
  def auth(token: String): Future[Unit] =
    doRequest(Auth(token)) {
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

  /**
   * Closes connection to Redis instance
   */
  def quit(): Future[Unit] =
    doRequest(Quit()) {
      case StatusReply(message) => Future.Unit
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
   * @return Sequence of field/value pairs
   */
  def hGetAllAsPairs(key: Array[Byte]): Future[Seq[(Array[Byte], Array[Byte])]] =
    doRequest(HGetAll(key)) {
      case MBulkReply(messages) => Future.value(returnPairs(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  @deprecated("Use hGetAllAsPairs instead", "5.0.0")
  def hGetAll(key: Array[Byte]): Future[Map[Array[Byte], Array[Byte]]] =
    hGetAllAsPairs(key) map { res => res toMap }

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
   * @return Score of member
   */
  def zScore(key: Array[Byte], member: Array[Byte]): Future[Option[Double]] =
    doRequest(ZScore(key, member)) {
      case BulkReply(message)   => Future.value(
        Some(NumberFormat.toDouble(BytesToString(message))))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets the rank of member in the sorted set, or None if it doesn't exist, from high to low.
   * @params key, member
   * @return the rank of the member
   */
  def zRevRank(key: Array[Byte], member: Array[Byte]): Future[Option[Int]] =
    doRequest(ZRevRank(BytesToString(key), member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply()   => Future.value(None)
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
   * Increment the member in sorted set key by amount.
   * Returns an option, None if the member is not found, or the set is empty, or the new value.
   * Throws an exception if the key refers to a structure that is not a sorted set.
   * @params key, amount, member
   * @return the new value of the incremented member
   */
  def zIncrBy(key: Array[Byte], amount: Double, member: Array[Byte]): Future[Option[Double]] =
    doRequest(ZIncrBy(BytesToString(key), amount, member)) {
      case BulkReply(message) => Future.value(Some(NumberFormat.toDouble(BytesToString(message))))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Gets the rank of the member in the sorted set, or None if it doesn't exist, from low to high.
   * @params, key, member
   * @return the rank of the member
   */
  def zRank(key: Array[Byte], member: Array[Byte]): Future[Option[Int]] =
    doRequest(ZRank(BytesToString(key), member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Gets member, score pairs from sorted set between min and max
   * Results are limited by offset and count
   * @param key, min, max, offset, count
   * @return ZRangeResults object containing item/score pairs
   */
  def zRangeByScore(
    key: Array[Byte], min: Double, max: Double, offset: Int, count: Int
  ): Future[ZRangeResults] =
    doRequest(
      ZRangeByScore(
        BytesToString(key),
        ZInterval(min),
        ZInterval(max),
        WithScores.asArg,
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(ZRangeResults(returnPairs(messages)))
      case EmptyMBulkReply()    => Future.value(ZRangeResults(List()))
    }

  @deprecated("Use zRangeByScore instead", "5.0.0")
  def zRangeByScoreWithScores(
    key: Array[Byte], min: Double, max: Double, offset: Int, count: Int
  ): Future[Map[Array[Byte], Array[Byte]]] =
    doRequest(
      ZRangeByScore(
        BytesToString(key),
        ZInterval(min),
        ZInterval(max),
        WithScores.asArg,
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(returnPairs(messages) toMap)
      case EmptyMBulkReply()    => Future.value(Map())
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
   * Removes members from sorted set by sort order, from start to stop, inclusive.
   * @params key, start, stop
   * @return Number of members removed from sorted set.
   */
  def zRemRangeByRank(key: Array[Byte], start: Int, stop: Int): Future[Int] =
    doRequest(ZRemRangeByRank(BytesToString(key), start, stop)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes members from sorted set by score, from min to max, inclusive.
   * @params key, min, max
   * @return Number of members removed from sorted set.
   */
  def zRemRangeByScore(key: Array[Byte], min: Double, max: Double): Future[Int] =
    doRequest(ZRemRangeByScore(BytesToString(key), ZInterval(min), ZInterval(max))) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key.
   * Elements are ordered from lowest to highest score.
   * @param key, start, stop
   * @return ZRangeResults object containing item/score pairs
   */
  def zRange(key: Array[Byte], start: Int, stop: Int): Future[Seq[Array[Byte]]] =
    doRequest(ZRange(BytesToString(key), start, stop)) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns specified range of elements in sorted set at key
   * Elements are ordered from highest to lowest score
   * @param key, start, stop
   * @return List of elements in specified range
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
   * @return ZRangeResults object containing item/score pairs
   */
  def zRevRangeByScore(
    key: Array[Byte], max: Double, min: Double, offset: Int, count: Int
  ): Future[ZRangeResults] =
    doRequest(
      ZRevRangeByScore(
        BytesToString(key),
        ZInterval(max),
        ZInterval(min),
        WithScores.asArg,
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(ZRangeResults(returnPairs(messages)))
      case EmptyMBulkReply()    => Future.value(ZRangeResults(List()))
    }

  @deprecated("Use zRevRangeByScore instead", "5.0.0")
  def zRevRangeByScoreWithScores(
    key: Array[Byte], max: Double, min: Double, offset: Int, count: Int
  ): Future[Map[Array[Byte], Array[Byte]]] =
    doRequest(
      ZRevRangeByScore(
        BytesToString(key),
        ZInterval(max),
        ZInterval(min),
        WithScores.asArg,
        Some(Limit(offset, count))
      )
    ) {
      case MBulkReply(messages) => Future.value(returnPairs(messages) toMap)
      case EmptyMBulkReply()    => Future.value(Map())
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

  /**
   * Helper function to convert a Redis multi-bulk reply into a map of pairs
   */
  private def returnPairs(messages: List[Array[Byte]]) = {
    assert(messages.length % 2 == 0, "Odd number of items in response")
    messages.grouped(2).toList flatMap { case List(a, b) => Some(a, b); case _ => None }
  }

}