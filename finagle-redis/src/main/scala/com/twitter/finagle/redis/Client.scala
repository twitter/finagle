package com.twitter.finagle.redis

import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat, ReplyFormat}
import com.twitter.finagle.{Service, ServiceFactory}
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
  def append(key: String, value: Array[Byte]): Future[Long] =
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
  def decrBy(key: String, amount: Int): Future[Long] =
    doRequest(DecrBy(key, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Increment number stored at key by one. If key doesn't
   * exist, value is set to 0 before the operation
   * @params key
   * @return Value after i. Error if key contains value
   * of the wrong type
   */
  def incr(key: String): Future[Int] =
    doRequest(Incr(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value associated with the given key
   * @param key
   * @return Option containing either the value, or nothing
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
  def del(keys: Seq[String]): Future[Long] =
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
   * Returns all keys matching pattern
   * @param pattern
   * @return List of keys matching pattern
   */
  def keys(pattern: String): Future[Seq[Array[Byte]]] =
    doRequest(Keys(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toByteArrays(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
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
  def hDel(key: String, fields: Seq[String]): Future[Long] =
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
     case MBulkReply(messages) => Future.value(returnPairs(ReplyFormat.toByteArrays(messages)))
     case EmptyMBulkReply()    => Future.value(Seq())
   }

  @deprecated("Use hGetAllAsPairs instead", "5.0.0")
  def hGetAll(key: Array[Byte]): Future[Map[Array[Byte], Array[Byte]]] =
    hGetAllAsPairs(key) map { res => res toMap }

  /**
   * Return all field names stored at key
   * @param hash key
   * @return List of fields in hash
   */
  def hKeys(key: String): Future[Seq[Array[Byte]]] =
   doRequest(HKeys(key)) {
     case MBulkReply(messages) => Future.value(ReplyFormat.toByteArrays(messages))
     case EmptyMBulkReply()    => Future.value(Seq())
   }

  /**
   * Gets values for given fields in hash
   * @param hash key, fields
   * @return List of values
   */
  def hMGet(key: String, fields: Seq[String]): Future[Seq[Array[Byte]]] =
   doRequest(HMGet(key, fields)) {
     case MBulkReply(messages) => Future.value(ReplyFormat.toByteArrays(messages))
     case EmptyMBulkReply()    => Future.value(Seq())
   }

  /**
   * Sets field value pair in given hash
   * @param hash key, field, value
   * @return 1 if field is new, 0 if field was updated
   */
  def hSet(key: Array[Byte], field: Array[Byte], value: Array[Byte]): Future[Long] =
    doRequest(HSet(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /** Sorted Set commands */

  /**
   * Adds member, score pair to sorted set
   * @params key, score, member
   * @return Number of elements added to sorted set
   */
  def zAdd(key: Array[Byte], score: Double, member: Array[Byte]): Future[Long] =
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
   * Gets number of elements in sorted set with score between min and max
   * @params key, min, max
   * @return Number of elements between min and max in sorted set
   */
  def zCount(key: Array[Byte], min: Double, max: Double): Future[Long] =
    doRequest(ZCount(key, ZInterval(min), ZInterval(max))) {
      case IntegerReply(n) => Future.value(n)
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
      case MBulkReply(messages) => Future.value(
        ZRangeResults(returnPairs(ReplyFormat.toByteArrays(messages))))
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
      case MBulkReply(messages) => Future.value(
        returnPairs(ReplyFormat.toByteArrays(messages)) toMap)
      case EmptyMBulkReply()    => Future.value(Map())
    }


  /**
   * Returns sorted set cardinality of the sorted set at key
   * @param key
   * @return Integer representing cardinality of sorted set,
   * or 0 if key does not exist
   */
  def zCard(key: Array[Byte]): Future[Long] =
    doRequest(ZCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes specified member(s) from sorted set at key
   * @params key, member(s)
   * @return Number of members removed from sorted set
   */
  def zRem(key: Array[Byte], members: Seq[Array[Byte]]): Future[Long] =
    doRequest(ZRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key
   * Elements are ordered from highest to lowest score
   * @param key, start, stop
   * @return List of elements in specified range
   */
  def zRevRange(key: Array[Byte], start: Int, stop: Int): Future[Seq[Array[Byte]]] =
    doRequest(ZRevRange(key, start, stop)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toByteArrays(messages))
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
      case MBulkReply(messages) => Future.value(
        ZRangeResults(returnPairs(ReplyFormat.toByteArrays(messages))))
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
      case MBulkReply(messages) => Future.value(
        returnPairs(ReplyFormat.toByteArrays(messages)) toMap)
      case EmptyMBulkReply()    => Future.value(Map())
    }

  def lPush(key: String, value: Array[Byte]): Future[Int] =
    doRequest(LPush(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  def lRange(key: String, start: Int, end: Int): Future[Seq[Array[Byte]]] =
    doRequest(LRange(key, start, end)) {
      case MBulkReply(messages) => Future.value(messages)
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  def sAdd(key: String, value: Array[Byte]): Future[Int] =
    doRequest(SAdd(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  def sRem(key: String, value: Array[Byte]): Future[Int] =
    doRequest(SRem(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  def sIsMember(key: String, value: Array[Byte]): Future[Int] =
    doRequest(SIsMember(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  def sMembers(key: String): Future[Seq[Array[Byte]]] =
    doRequest(SMembers(key)) {
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
  private[redis] def doRequest[T](cmd: Command)(handler: PartialFunction[Reply, Future[T]]) =
    service(cmd) flatMap (handler orElse {
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    })

  /**
   * Helper function to convert a Redis multi-bulk reply into a map of pairs
   */
  private[redis] def returnPairs(messages: List[Array[Byte]]) = {
    assert(messages.length % 2 == 0, "Odd number of items in response")
    messages.grouped(2).toList flatMap { case List(a, b) => Some(a, b); case _ => None }
  }

}

/**
 * Client connected over a single connection to a
 * single redis instance, supporting transactions
 */
trait TransactionalClient extends Client {

  /**
   * Flushes all previously watched keys for a transaction
   */
  def unwatch(): Future[Unit]

  /**
   * Marks given keys to be watched for conditional execution of a transaction
   * @param keys to watch
   */
  def watch(keys: Seq[Array[Byte]]): Future[Unit]

  /**
   * Executes given vector of commands as a Redis transaction
   * The order of the commands queued on the Redis server is not guaranteed
   * @param Sequence of commands to be executed
   * @return Results of each command in order
   */
  def transaction(cmds: Seq[Command]): Future[Seq[Reply]]

}

object TransactionalClient {

  /**
   * Construct a client from a single host with transaction commands
   * @param host a String of host:port combination.
   */
  def apply(host: String): TransactionalClient = TransactionalClient(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Redis())
      .buildFactory())

  /**
   * Construct a client from a service factory
   * @param raw ServiceFactory
   */
  def apply(raw: ServiceFactory[Command, Reply]): TransactionalClient =
    new ConnectedTransactionalClient(raw)

}

/**
 * Connects to a single Redis host supporting transactions
 */
private[redis] class ConnectedTransactionalClient(
  serviceFactory: ServiceFactory[Command, Reply]
) extends Client(serviceFactory.toService) with TransactionalClient {

  def unwatch(): Future[Unit] =
    doRequest(UnWatch()) {
      case StatusReply(message)  => Future.Unit
    }

  def watch(keys: Seq[Array[Byte]]): Future[Unit] =
    doRequest(Watch(keys.toList)) {
      case StatusReply(message)  => Future.Unit
    }

  def transaction(cmds: Seq[Command]): Future[Seq[Reply]] = {
    serviceFactory() flatMap { svc =>
      multi(svc) flatMap { _ =>
        val cmdQueue = cmds.toList map { cmd => svc(cmd) }
        Future.collect(cmdQueue) flatMap { _ => exec(svc) }
      } rescue { case e =>
        svc(Discard()) flatMap { _ =>
          Future.exception(ClientError("Transaction failed: " + e.toString))
        }
      } ensure {
        svc.release()
      }
    }
  }

  private def multi(svc: Service[Command, Reply]): Future[Unit] =
    svc(Multi()) flatMap {
      case StatusReply(message)  => Future.Unit
      case ErrorReply(message)   => Future.exception(new ServerError(message))
      case _                     => Future.exception(new IllegalStateException)
  }

  private def exec(svc: Service[Command, Reply]): Future[Seq[Reply]] =
    svc(Exec()) flatMap {
      case MBulkReply(messages)  => Future.value(messages)
      case EmptyMBulkReply()     => Future.value(Seq())
      case NilMBulkReply()       => Future.exception(
        new ServerError("One or more keys were modified before transaction"))
      case ErrorReply(message)   => Future.exception(new ServerError(message))
      case _                     => Future.exception(new IllegalStateException)
    }

}