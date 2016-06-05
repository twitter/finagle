package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer


trait Strings { self: BaseClient =>
  private[this] val unwrapBufOption: Option[Buf] => Option[ChannelBuffer] =
    _.map(BufChannelBuffer(_))

  private[this] val unwrapSeqBufOption: Seq[Option[Buf]] => Seq[Option[ChannelBuffer]] =
    _.map(unwrapBufOption)

  private[this] val wrapCB: ChannelBuffer => Buf = ChannelBufferBuf.Owned(_)

  private[this] val wrapCBPair: ((ChannelBuffer, ChannelBuffer)) => (Buf, Buf) =
    { x => ChannelBufferBuf.Owned(x._1) -> ChannelBufferBuf.Owned(x._2)}

  val FutureTrue: Future[JBoolean] = Future.value(true)
  val FutureFalse: Future[JBoolean] = Future.value(false)

  /**
   * Appends value at the given key. If key doesn't exist,
   * behavior is similar to SET command
   *
   * @param key
   * @param value
   * @return length of string after append operation
   */
  def append(key: Buf, value: Buf): Future[JLong] =
    doRequest(Append(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use append(key: Buf, value: Buf)", "2016-03-31")
  def append(key: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    append(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value))

  /**
   * Count the number of set bits (population counting) in a string.
   *
   * @param key
   * @param start optional index bytes starting from.
   * @param end   optional index bytes of the end.
   * @return The number of bits set to 1.
   * @see http://redis.io/commands/bitcount
   */
  @deprecated("use bitCount(key: Buf, start: Option[Int], end: Option[Int])", "2016-03-31")
  def bitCount(key: ChannelBuffer, start: Option[Int] = None, end: Option[Int] = None): Future[JLong] =
    bitCount(ChannelBufferBuf.Owned(key), start, end)

  def bitCount(key: Buf): Future[JLong] =
    doRequest(BitCount(key, None, None)) {
      case IntegerReply(n) => Future.value(n)
    }

  def bitCount(key: Buf, start: Option[Int], end: Option[Int]): Future[JLong] =
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
  @deprecated("use bitOp(op: Buf, dstKey: Buf, srcKeys: Seq[Buf])", "2016-03-31")
  def bitOp(op: ChannelBuffer, dstKey: ChannelBuffer, srcKeys: Seq[ChannelBuffer]): Future[JLong] =
    bitOp(ChannelBufferBuf.Owned(op), ChannelBufferBuf.Owned(dstKey), srcKeys.map(ChannelBufferBuf.Owned(_)))

  def bitOp(op: Buf, dstKey: Buf, srcKeys: Seq[Buf]): Future[JLong] =
    doRequest(BitOp(op, dstKey, srcKeys)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use decr(key: Buf)", "2016-03-31")
  def decr(key: ChannelBuffer): Future[JLong] =
    decr(ChannelBufferBuf.Owned(key))

  /**
   * Decrements number stored at key by 1.
   *
   * @param key
   * @return value after decrement.
   */
  def decr(key: Buf): Future[JLong] =
    doRequest(Decr(key)) {
      case IntegerReply(n) => Future.value(n)
    }


  @deprecated("use decrBy(key: Buf, amount: Long)", "2016-03-31")
  def decrBy(key: ChannelBuffer, amount: Long): Future[JLong] =
    decrBy(ChannelBufferBuf.Owned(key), amount)

  /**
   * Decrements number stored at key by given amount. If key doesn't
   * exist, value is set to 0 before the operation
   *
   * @param key
   * @param amount
   * @return value after decrement. Error if key contains value
   * of the wrong type
   */
  def decrBy(key: Buf, amount: Long): Future[JLong] =
    doRequest(DecrBy(key, amount)) {
      case IntegerReply(n) => Future.value(n)
    }



  @deprecated("use get(key: Buf)", "2016-03-31")
  def get(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    get(ChannelBufferBuf.Owned(key)).map(unwrapBufOption)

  /**
   * Gets the value associated with the given key
   *
   * @param key
   * @return value, or none if key doesn't exist
   */
  def get(key: Buf): Future[Option[Buf]] =
    doRequest(Get(key)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
  }

  @deprecated("use getBit(key: Buf, offset: Int)", "2016-03-31")
  def getBit(key: ChannelBuffer, offset: Int): Future[JLong] =
    getBit(ChannelBufferBuf.Owned(key), offset)


  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key, offset
   * @return the bit value stored at offset.
   * @see http://redis.io/commands/getbit
   */
  def getBit(key: Buf, offset: Int): Future[JLong] =
    doRequest(GetBit(key, offset)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use getRange(key: Buf, start: Long, end: Long)", "2016-03-31")
  def getRange(key: ChannelBuffer, start: Long, end: Long): Future[Option[ChannelBuffer]] =
    getRange(ChannelBufferBuf.Owned(key), start, end).map(unwrapBufOption)

  /**
   * Gets the substring of the value associated with given key
   *
   * @param key
   * @param start
   * @param end
   * @return substring, or none if key doesn't exist
   */
  def getRange(key: Buf, start: Long, end: Long): Future[Option[Buf]] =
    doRequest(GetRange(key, start, end)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
  }

  @deprecated("use getSet(key: Buf, value: Buf)", "2016-03-31")
  def getSet(key: ChannelBuffer, value: ChannelBuffer): Future[Option[ChannelBuffer]] =
    getSet(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value)).map(unwrapBufOption)


  /**
   * Atomically sets key to value and returns the old value stored at key.
   * Returns an error when key exists but does not hold a string value.
   *
   * @param key, value
   * @return the old value stored at key wrapped in Some,
   *          or None when key did not exist.
   * @see http://redis.io/commands/getset
   */
  def getSet(key: Buf, value: Buf): Future[Option[Buf]] =
    doRequest(GetSet(key, value)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
  }

  @deprecated("use incr(key: Buf)", "2016-03-31")
  def incr(key: ChannelBuffer): Future[JLong] =
    incr(ChannelBufferBuf.Owned(key))

  /**
   * Increments the number stored at key by one.
   *
   * @param key
   * @return the value of key after the increment.
   * @see http://redis.io/commands/incr
   */
  def incr(key: Buf): Future[JLong] =
    doRequest(Incr(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use incrBy(key: Buf, increment: Long)", "2016-03-31")
  def incrBy(key: ChannelBuffer, increment: Long): Future[JLong] =
    incrBy(ChannelBufferBuf.Owned(key), increment)


  /**
   * Increments the number stored at key by increment.
   *
   * @param key, increment
   * @return the value of key after the increment.
   * @see http://redis.io/commands/incrby
   */
  def incrBy(key: Buf, increment: Long): Future[JLong] =
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
  def mGet(keys: Seq[Buf]): Future[Seq[Option[Buf]]] =
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

  @deprecated("use mGet(keys: Seq[Buf])", "2016-03-31")
  def mGet2(keys: Seq[ChannelBuffer]): Future[Seq[Option[ChannelBuffer]]] =
    mGet(keys.map(wrapCB)).map(unwrapSeqBufOption)

  /**
   * Sets the given keys to their respective values. MSET replaces existing
   * values with new values, just as regular SET.
   *
   * @param kv
   * @see http://redis.io/commands/mset
   */
  def mSet(kv: Map[Buf, Buf]): Future[Unit] =
    doRequest(MSet(kv)) {
      case StatusReply(message) => Future.Unit
    }

  @deprecated("use mSet(kv: Map[Buf, Buf])", "2016-03-31")
  def mSet2(kv: Map[ChannelBuffer, ChannelBuffer]): Future[Unit] =
    mSet(kv.map(wrapCBPair))

  /**
   * Sets the given keys to their respective values. MSETNX will not perform
   * any operation at all even if just a single key already exists.
   *
   * @param kv
   * @return 1 if all keys were set, 0 if no keys were set.
   * @see http://redis.io/commands/msetnx
   */
  def mSetNx(kv: Map[Buf, Buf]): Future[JBoolean] =
    doRequest(MSetNx(kv)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  @deprecated("use mSetNx(kv: Map[Buf, Buf])", "2016-03-31")
  def mSetNx2(kv: Map[ChannelBuffer, ChannelBuffer]): Future[JBoolean] =
    mSetNx(kv.map(wrapCBPair))

  @deprecated("use pSetEx(key: Buf, millis: Long, value: Buf)", "2016-03-31")
  def pSetEx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[Unit] =
    pSetEx(ChannelBufferBuf.Owned(key), millis, ChannelBufferBuf.Owned(value))

  /**
   * Works exactly like SETEX with the sole difference that the expire
   * time is specified in milliseconds instead of seconds.
   *
   * @param key, millis
   * @see http://redis.io/commands/psetex
   */
  def pSetEx(key: Buf, millis: Long, value: Buf): Future[Unit] =
    doRequest(PSetEx(key, millis, value)) {
      case StatusReply(message) => Future.Unit
    }

  @deprecated("use set(key: Buf, value: Buf)", "2016-03-31")
  def set(key: ChannelBuffer, value: ChannelBuffer): Future[Unit] =
    set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value))

  /**
   * Sets the given value to key. If a value already exists for the key,
   * the value is overwritten with the new value
   *
   * @param key
   * @param value
   */
  def set(key: Buf, value: Buf): Future[Unit] =
    doRequest(Set(key, value)) {
      case StatusReply(message) => Future.Unit
    }

  @deprecated("use setBit(key: Buf, offset: Int, value: Int)", "2016-03-31")
  def setBit(key: ChannelBuffer, offset: Int, value: Int): Future[JLong] =
    setBit(ChannelBufferBuf.Owned(key), offset, value)


  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @param key, offset, value
   * @return the original bit value stored at offset.
   * @see http://redis.io/commands/setbit
   */
  def setBit(key: Buf, offset: Int, value: Int): Future[JLong] =
    doRequest(SetBit(key, offset, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use setEx(key: Buf, seconds: Long, value: Buf)", "2016-03-31")
  def setEx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[Unit] =
    setEx(ChannelBufferBuf.Owned(key), seconds, ChannelBufferBuf.Owned(value))

  /**
   * Set key to hold the string value and set key to timeout after a given
   * number of seconds.
   *
   * @param key, seconds, value
   * @see http://redis.io/commands/setex
   */
  def setEx(key: Buf, seconds: Long, value: Buf): Future[Unit] =
    doRequest(SetEx(key, seconds, value)) {
      case StatusReply(message) => Future.Unit
    }

  @deprecated("use setExNx(key: Buf, seconds: Long, value: Buf)", "2016-03-31")
  def setExNx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), Some(InSeconds(seconds)), true, false)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
    }

  /**
   * Set key to hold the string value with the specified expire time in seconds
   * only if the key does not already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setExNx(key: Buf, seconds: Long, value: Buf): Future[JBoolean] =
    doRequest(Set(key, value, Some(InSeconds(seconds)), true, false)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
  }

  @deprecated("use setExXx(key: Buf, seconds: Long, value: Buf)", "2016-03-31")
  def setExXx(key: ChannelBuffer, seconds: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), Some(InSeconds(seconds)), false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
    }

  /**
   * Set key to hold the string value with the specified expire time in seconds
   * only if the key already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setExXx(key: Buf, seconds: Long, value: Buf): Future[JBoolean] =
    doRequest(Set(key, value, Some(InSeconds(seconds)), false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
  }

  @deprecated("use setNx(key: Buf, value: Buf)", "2016-03-31")
  def setNx(key: ChannelBuffer, value: ChannelBuffer): Future[JBoolean] =
    setNx(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value))

  /**
   * Set key to hold string value if key does not exist. In that case, it is
   * equal to SET. When key already holds a value, no operation is performed.
   *
   * @param key, value
   * @return 1 if the key was set, 0 if the key was not set.
   * @see http://redis.io/commands/setnx
   */
  def setNx(key: Buf, value: Buf): Future[JBoolean] =
    doRequest(SetNx(key, value)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  @deprecated("use setPx(key: Buf, millis: Long, value: Buf)", "2016-03-31")
  def setPx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[Unit] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), Some(InMilliseconds(millis)))) {
      case StatusReply(_) => Future.Unit
    }


  /**
   * Set key to hold the string value with the specified expire time in milliseconds.
   *
   * @param key, millis, value
   * @see http://redis.io.commands/set
   */
  def setPx(key: Buf, millis: Long, value: Buf): Future[Unit] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)))) {
      case StatusReply(_) => Future.Unit
    }

  @deprecated("use setPxNx(key: Buf, millis: Long, value: Buf)", "2016-03-31")
  def setPxNx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), Some(InMilliseconds(millis)), true, false)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
    }

  /**
   * Set key to hold the string value with the specified expire time in milliseconds
   * only if the key does not already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setPxNx(key: Buf, millis: Long, value: Buf): Future[JBoolean] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)), true, false)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
  }

  @deprecated("use setPxXx(key: Buf, millis: Long, value: Buf)", "2016-03-31")
  def setPxXx(key: ChannelBuffer, millis: Long, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), Some(InMilliseconds(millis)), false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
    }

  /**
   * Set key to hold the string value with the specified expire time in milliseconds
   * only if the key already exist.
   *
   * @param key, millis, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setPxXx(key: Buf, millis: Long, value: Buf): Future[JBoolean] =
    doRequest(Set(key, value, Some(InMilliseconds(millis)), false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
  }

  @deprecated("use setXx(key: Buf, value: Buf)", "2016-03-31")
  def setXx(key: ChannelBuffer, value: ChannelBuffer): Future[JBoolean] =
    doRequest(Set(ChannelBufferBuf.Owned(key), ChannelBufferBuf.Owned(value), None, false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
    }

  /**
   * Set key to hold the string value only if the key already exist.
   *
   * @param key, value
   * @return true if the key was set, false if condition was not met.
   * @see http://redis.io.commands/set
   */
  def setXx(key: Buf, value: Buf): Future[JBoolean] =
    doRequest(Set(key, value, None, false, true)) {
      case StatusReply(_) => FutureTrue
      case EmptyBulkReply() => FutureFalse
  }

  @deprecated("use setRange(key: Buf, offset: Int, value: Buf)", "2016-03-31")
  def setRange(key: ChannelBuffer, offset: Int, value: ChannelBuffer): Future[JLong] =
    setRange(ChannelBufferBuf.Owned(key), offset, ChannelBufferBuf.Owned(value))

  /**
   * Overwrites part of the string stored at key, starting at the specified
   * offset, for the entire length of value.
   *
   * @param key, offset, value
   * @return the length of the string after it was modified.
   * @see http://redis.io/commands/setrange
   */
  def setRange(key: Buf, offset: Int, value: Buf): Future[JLong] =
    doRequest(SetRange(key, offset, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  @deprecated("use strlen(key: Buf)", "2016-03-31")
  def strlen(key: ChannelBuffer): Future[JLong] =
    strlen(ChannelBufferBuf.Owned(key))

  /**
   * returns the length of the string value stored at key.
   *
   * @param key
   * @return the length of the string at key, or 0 when key does not exist.
   * @see http://redis.io/commands/strlen
   */
  def strlen(key: Buf): Future[JLong] =
    doRequest(Strlen(key)) {
      case IntegerReply(n) => Future.value(n)
    }
}
