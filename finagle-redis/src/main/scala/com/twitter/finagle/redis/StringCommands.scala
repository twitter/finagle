package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

trait Strings { self: BaseClient =>

  /**
   * Appends value at the given key. If key doesn't exist,
   * behavior is similar to SET command
   * @param key
   * @param value
   * @return length of string after append operation
   */
  def append(key: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    doRequest(Append(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Count the number of set bits (population counting) in a string.
   *
   * @param key
   * @param start optional index bytes starting from.
   * @param end   optional index bytes of the end.
   * @return The number of bits set to 1.
   * @see http://redis.io/commands/bitcount
   */
  def bitCount(key: ChannelBuffer, start: Option[Int] = None,
      end: Option[Int] = None): Future[JLong] =
    doRequest(BitCount(key, start, end)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Perform a bitwise operation between multiple keys (containing string
   * values) and store the result in the destination key.
   *
   * @param op      operation type, one of AND/OR/XOR/NOT.
   * @param dstKey  destination key.
   * @param srcKeys source keys perform the operation between.
   * @return The size of the string stored in the destination key,
   *          that is equal to the size of the longest input string.
   * @see http://redis.io/commands/bitop
   */
  def bitOp(op: ChannelBuffer, dstKey: ChannelBuffer,
      srcKeys: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(BitOp(op, dstKey, srcKeys)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Decremts number stored at key by 1.
   * @param key
   * @return value after decrement.
   */
  def decr(key: ChannelBuffer): Future[JLong] =
    doRequest(Decr(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Decrements number stored at key by given amount. If key doesn't
   * exist, value is set to 0 before the operation
   * @param key
   * @param amount
   * @return value after decrement. Error if key contains value
   * of the wrong type
   */
  def decrBy(key: ChannelBuffer, amount: Long): Future[JLong] =
    doRequest(DecrBy(key, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value associated with the given key
   * @param key
   * @return value, or none if key doesn't exist
   */
  def get(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(Get(key)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key, offset
   * @return the bit value stored at offset.
   * @see http://redis.io/commands/getbit
   */
  def getBit(key: ChannelBuffer, offset: Int): Future[JLong] =
    doRequest(GetBit(key, offset)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the substring of the value associated with given key
   * @param key
   * @param start
   * @param end
   * @return substring, or none if key doesn't exist
   */
  def getRange(key: ChannelBuffer, start: Long, end: Long): Future[Option[ChannelBuffer]] =
    doRequest(GetRange(key, start, end)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Atomically sets key to value and returns the old value stored at key.
   * Returns an error when key exists but does not hold a string value.
   *
   * @param key, value
   * @return the old value stored at key wrapped in Some,
   *          or None when key did not exist.
   * @see http://redis.io/commands/getset
   */
  def getSet(key: ChannelBuffer, value: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(GetSet(key, value)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Increments the number stored at key by one.
   *
   * @param key
   * @return the value of key after the increment.
   * @see http://redis.io/commands/incr
   */
  def incr(key: ChannelBuffer): Future[JLong] =
    doRequest(Incr(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Increments the number stored at key by increment.
   *
   * @param key, increment
   * @return the value of key after the increment.
   * @see http://redis.io/commands/incrby
   */
  def incrBy(key: ChannelBuffer, increment: Long): Future[JLong] =
    doRequest(IncrBy(key, increment)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns the values of all specified keys.
   *
   * @param keys
   * @return list of values at the specified keys.
   * @see http://redis.io/commands/mget
   */
  def mGet(keys: Seq[ChannelBuffer]): Future[Seq[Option[ChannelBuffer]]] =
    doRequest(MGet(keys)) {
      case MBulkReply(messages) => Future {
        messages.map {
          case BulkReply(message) => Some(message)
          case EmptyBulkReply()   => None
          case _ => throw new IllegalStateException()
        }.toSeq
      }
      case EmptyMBulkReply()    => Future.Nil
    }

  /**
   * Sets the given keys to their respective values. MSET replaces existing
   * values with new values, just as regular SET.
   *
   * @param kv
   * @see http://redis.io/commands/mset
   */
  def mSet(kv: Map[ChannelBuffer, ChannelBuffer]): Future[Unit] =
    doRequest(MSet(kv)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Sets the given keys to their respective values. MSETNX will not perform
   * any operation at all even if just a single key already exists.
   *
   * @param kv
   * @return 1 if all keys were set, 0 if no keys were set.
   * @see http://redis.io/commands/msetnx
   */
  def mSetNx(kv: Map[ChannelBuffer, ChannelBuffer]): Future[JBoolean] =
    doRequest(MSetNx(kv)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Works exactly like SETEX with the sole difference that the expire
   * time is specified in milliseconds instead of seconds.
   *
   * @param key, millis
   * @see http://redis.io/commands/psetex
   */
  def pSetEx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[Unit] =
    doRequest(PSetEx(key, millis, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Sets the given value to key. If a value already exists for the key,
   * the value is overwritten with the new value
   * @param key
   * @param value
   */
  def set(key: ChannelBuffer, value: ChannelBuffer): Future[Unit] =
    doRequest(Set(key, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @param key, offset, value
   * @return the original bit value stored at offset.
   * @see http://redis.io/commands/setbit
   */
  def setBit(key: ChannelBuffer, offset: Int, value: Int): Future[JLong] =
    doRequest(SetBit(key, offset, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Set key to hold the string value and set key to timeout after a given
   * number of seconds.
   *
   * @param key, seconds, value
   * @see http://redis.io/commands/setex
   */
  def setEx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[Unit] =
    doRequest(SetEx(key, seconds, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Set key to hold the string value with the specified expire time in seconds
   * only if the key does not already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setExNx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(key, value, Some(InSeconds(seconds)), true, false)) {
      case StatusReply(_) => Future.value(true)
      case EmptyBulkReply() => Future.value(false)
    }

  /**
   * Set key to hold the string value with the specified expire time in seconds
   * only if the key already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setExXx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(key, value, Some(InSeconds(seconds)), false, true)) {
      case StatusReply(_) => Future.value(true)
      case EmptyBulkReply() => Future.value(false)
    }

  /**
   * Set key to hold string value if key does not exist. In that case, it is
   * equal to SET. When key already holds a value, no operation is performed.
   *
   * @param key, value
   * @return 1 if the key was set, 0 if the key was not set.
   * @see http://redis.io/commands/setnx
   */
  def setNx(key: ChannelBuffer, value: ChannelBuffer): Future[JBoolean] =
    doRequest(SetNx(key, value)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Set key to hold the string value with the specified expire time in milliseconds.
   *
   * @param key, millis, value
   * @see http://redis.io.commands/set
   */
  def setPx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[Unit] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)))) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Set key to hold the string value with the specified expire time in milliseconds
   * only if the key does not already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setPxNx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)), true, false)) {
      case StatusReply(_) => Future.value(true)
      case EmptyBulkReply() => Future.value(false)
    }

  /**
   * Set key to hold the string value with the specified expire time in milliseconds
   * only if the key already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setPxXx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)), false, true)) {
      case StatusReply(_) => Future.value(true)
      case EmptyBulkReply() => Future.value(false)
    }

  /**
   * Set key to hold the string value only if the key already exist.
   *
   * @param key, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setXx(key: ChannelBuffer, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(key, value, None, false, true)) {
      case StatusReply(_) => Future.value(true)
      case EmptyBulkReply() => Future.value(false)
    }

  /**
   * Overwrites part of the string stored at key, starting at the specified
   * offset, for the entire length of value.
   *
   * @param key, offset, value
   * @return the length of the string after it was modified.
   * @see http://redis.io/commands/setrange
   */
  def setRange(key: ChannelBuffer, offset: Int, value: ChannelBuffer): Future[JLong] =
    doRequest(SetRange(key, offset, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * returns the length of the string value stored at key.
   *
   * @param key
   * @return the length of the string at key, or 0 when key does not exist.
   * @see http://redis.io/commands/strlen
   */
  def strlen(key: ChannelBuffer): Future[JLong] =
    doRequest(Strlen(key)) {
      case IntegerReply(n) => Future.value(n)
    }
}
