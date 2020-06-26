package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

object RequireClientProtocol extends ErrorConversion {
  override def getException(msg: String): Exception = ClientError(msg)
}

/**
 * Redis command.
 *
 * @see https://redis.io/commands
 */
abstract class Command {
  def name: Buf
  def body: Seq[Buf] = Seq.empty
}

object Command {

  // Common constants
  val EOL = Buf.Utf8("\r\n")
  val ARG_COUNT = Buf.Utf8("*")
  val ARG_SIZE = Buf.Utf8("$")

  // BTreeSorterSets
  val BADD = Buf.Utf8("BADD")
  val BCARD = Buf.Utf8("BCARD")
  val BREM = Buf.Utf8("BREM")
  val BGET = Buf.Utf8("BGET")
  val BRANGE = Buf.Utf8("BRANGE")
  val BMERGEEX = Buf.Utf8("BMERGEEX")

  // Transactions
  val DISCARD = Buf.Utf8("DISCARD")
  val EXEC = Buf.Utf8("EXEC")
  val MULTI = Buf.Utf8("MULTI")
  val UNWATCH = Buf.Utf8("UNWATCH")
  val WATCH = Buf.Utf8("WATCH")

  // Topology
  // These are twitter-internal commands and will be removed eventually
  val TOPOLOGYADD = Buf.Utf8("TOPOLOGYADD")
  val TOPOLOGYGET = Buf.Utf8("TOPOLOGYGET")
  val TOPOLOGYDELETE = Buf.Utf8("TOPOLOGYDELETE")

  // String Commands
  val APPEND = Buf.Utf8("APPEND")
  val BITCOUNT = Buf.Utf8("BITCOUNT")
  val BITOP = Buf.Utf8("BITOP")
  val DECR = Buf.Utf8("DECR")
  val DECRBY = Buf.Utf8("DECRBY")
  val GET = Buf.Utf8("GET")
  val GETBIT = Buf.Utf8("GETBIT")
  val GETRANGE = Buf.Utf8("GETRANGE")
  val GETSET = Buf.Utf8("GETSET")
  val INCR = Buf.Utf8("INCR")
  val INCRBY = Buf.Utf8("INCRBY")
  val MGET = Buf.Utf8("MGET")
  val MSET = Buf.Utf8("MSET")
  val MSETNX = Buf.Utf8("MSETNX")
  val PSETEX = Buf.Utf8("PSETEX")
  val SET = Buf.Utf8("SET")
  val SETBIT = Buf.Utf8("SETBIT")
  val SETEX = Buf.Utf8("SETEX")
  val SETNX = Buf.Utf8("SETNX")
  val SETRANGE = Buf.Utf8("SETRANGE")
  val STRLEN = Buf.Utf8("STRLEN")

  // Sorted Sets
  // Only supported in Twitter's internal Redis fork.
  val ZADD = Buf.Utf8("ZADD")
  val ZCARD = Buf.Utf8("ZCARD")
  val ZCOUNT = Buf.Utf8("ZCOUNT")
  val ZINCRBY = Buf.Utf8("ZINCRBY")
  val ZINTERSTORE = Buf.Utf8("ZINTERSTORE")
  val ZRANGE = Buf.Utf8("ZRANGE")
  val ZRANGEBYSCORE = Buf.Utf8("ZRANGEBYSCORE")
  val ZRANK = Buf.Utf8("ZRANK")
  val ZREM = Buf.Utf8("ZREM")
  val ZREMRANGEBYRANK = Buf.Utf8("ZREMRANGEBYRANK")
  val ZREMRANGEBYSCORE = Buf.Utf8("ZREMRANGEBYSCORE")
  val ZREVRANGE = Buf.Utf8("ZREVRANGE")
  val ZREVRANGEBYSCORE = Buf.Utf8("ZREVRANGEBYSCORE")
  val ZREVRANK = Buf.Utf8("ZREVRANK")
  val ZSCAN = Buf.Utf8("ZSCAN")
  val ZSCORE = Buf.Utf8("ZSCORE")
  val ZUNIONSTORE = Buf.Utf8("ZUNIONSTORE")
  val ZPOPMIN = Buf.Utf8("ZPOPMIN")
  val ZPOPMAX = Buf.Utf8("ZPOPMAX")

  // Sets
  val SADD = Buf.Utf8("SADD")
  val SMEMBERS = Buf.Utf8("SMEMBERS")
  val SISMEMBER = Buf.Utf8("SISMEMBER")
  val SCARD = Buf.Utf8("SCARD")
  val SREM = Buf.Utf8("SREM")
  val SPOP = Buf.Utf8("SPOP")
  val SRANDMEMBER = Buf.Utf8("SRANDMEMBER")
  val SINTER = Buf.Utf8("SINTER")
  val SSCAN = Buf.Utf8("SSCAN")

  // Miscellaneous
  val PING = Buf.Utf8("PING")
  val FLUSHALL = Buf.Utf8("FLUSHALL")
  val FLUSHDB = Buf.Utf8("FLUSHDB")
  val SELECT = Buf.Utf8("SELECT")
  val AUTH = Buf.Utf8("AUTH")
  val INFO = Buf.Utf8("INFO")
  val QUIT = Buf.Utf8("QUIT")
  val SLAVEOF = Buf.Utf8("SLAVEOF")
  val REPLICAOF = Buf.Utf8("REPLICAOF")
  val CONFIG = Buf.Utf8("CONFIG")
  val SENTINEL = Buf.Utf8("SENTINEL")
  val CLUSTER = Buf.Utf8("CLUSTER")
  val DBSIZE = Buf.Utf8("DBSIZE")

  // Scripts
  val EVAL = Buf.Utf8("EVAL")
  val EVALSHA = Buf.Utf8("EVALSHA")
  val SCRIPT = Buf.Utf8("SCRIPT")
  val FLUSH = Buf.Utf8("FLUSH")
  val LOAD = Buf.Utf8("LOAD")

  // PubSub
  val PUBLISH = Buf.Utf8("PUBLISH")
  val SUBSCRIBE = Buf.Utf8("SUBSCRIBE")
  val UNSUBSCRIBE = Buf.Utf8("UNSUBSCRIBE")
  val PSUBSCRIBE = Buf.Utf8("PSUBSCRIBE")
  val PUNSUBSCRIBE = Buf.Utf8("PUNSUBSCRIBE")
  val PUBSUB = Buf.Utf8("PUBSUB")

  // Lists
  val LLEN = Buf.Utf8("LLEN")
  val LINDEX = Buf.Utf8("LINDEX")
  val LINSERT = Buf.Utf8("LINSERT")
  val LPOP = Buf.Utf8("LPOP")
  val LPUSH = Buf.Utf8("LPUSH")
  val LREM = Buf.Utf8("LREM")
  val LRESET = Buf.Utf8("LRESET")
  val LSET = Buf.Utf8("LSET")
  val LRANGE = Buf.Utf8("LRANGE")
  val RPOP = Buf.Utf8("RPOP")
  val RPUSH = Buf.Utf8("RPUSH")
  val LTRIM = Buf.Utf8("LTRIM")
  val RPOPLPUSH = Buf.Utf8("RPOPLPUSH")

  // Key Commands
  val DEL = Buf.Utf8("DEL")
  val DUMP = Buf.Utf8("DUMP")
  val EXISTS = Buf.Utf8("EXISTS")
  val EXPIRE = Buf.Utf8("EXPIRE")
  val EXPIREAT = Buf.Utf8("EXPIREAT")
  val KEYS = Buf.Utf8("KEYS")
  val MIGRATE = Buf.Utf8("MIGRATE")
  val MOVE = Buf.Utf8("MOVE")
  val PERSIST = Buf.Utf8("PERSIST")
  val PEXPIRE = Buf.Utf8("PEXPIRE")
  val PEXPIREAT = Buf.Utf8("PEXPIREAT")
  val PTTL = Buf.Utf8("PTTL")
  val RANDOMKEY = Buf.Utf8("RANDOMKEY")
  val RENAME = Buf.Utf8("RENAME")
  val RENAMENX = Buf.Utf8("RENAMENX")
  val SCAN = Buf.Utf8("SCAN")
  val TTL = Buf.Utf8("TTL")
  val TYPE = Buf.Utf8("TYPE")

  // HyperLogLogs
  val PFADD = Buf.Utf8("PFADD")
  val PFCOUNT = Buf.Utf8("PFCOUNT")
  val PFMERGE = Buf.Utf8("PFMERGE")

  // Hash Sets
  val HDEL = Buf.Utf8("HDEL")
  val HEXISTS = Buf.Utf8("HEXISTS")
  val HGET = Buf.Utf8("HGET")
  val HGETALL = Buf.Utf8("HGETALL")
  val HINCRBY = Buf.Utf8("HINCRBY")
  val HKEYS = Buf.Utf8("HKEYS")
  val HLEN = Buf.Utf8("HLEN")
  val HMGET = Buf.Utf8("HMGET")
  val HMSET = Buf.Utf8("HMSET")
  val HMSETEX = Buf.Utf8("HMSETEX")
  val HMADDEX = Buf.Utf8("HMERGEEX")
  val HSCAN = Buf.Utf8("HSCAN")
  val HSET = Buf.Utf8("HSET")
  val HSETNX = Buf.Utf8("HSETNX")
  val HVALS = Buf.Utf8("HVALS")
  val HSTRLEN = Buf.Utf8("HSTRLEN")

  // Geo commands (only available in Redis >= 3.2)
  val GEOADD = Buf.Utf8("GEOADD")
  val GEOHASH = Buf.Utf8("GEOHASH")
  val GEORADIUS = Buf.Utf8("GEORADIUS")
  val GEORADIUSBYMEMBER = Buf.Utf8("GEORADIUSBYMEMBER")
  val GEOPOS = Buf.Utf8("GEOPOS")
  val GEODIST = Buf.Utf8("GEODIST")
  // Geo command arguments
  val WITHCOORD = Buf.Utf8("WITHCOORD")
  val WITHDIST = Buf.Utf8("WITHDIST")
  val WITHHASH = Buf.Utf8("WITHHASH")
  // Command Arguments
  val WITHSCORES = Buf.Utf8("WITHSCORES")
  val LIMIT = Buf.Utf8("LIMIT")
  val WEIGHTS = Buf.Utf8("WEIGHTS")
  val AGGREGATE = Buf.Utf8("AGGREGATE")
  val COUNT = Buf.Utf8("COUNT")
  val MATCH = Buf.Utf8("MATCH")

  // Streams
  val XINFO = Buf.Utf8("XINFO")
  val XADD = Buf.Utf8("XADD")
  val XTRIM = Buf.Utf8("XTRIM")
  val XDEL = Buf.Utf8("XDEL")
  val XRANGE = Buf.Utf8("XRANGE")
  val XREVRANGE = Buf.Utf8("XREVRANGE")
  val XLEN = Buf.Utf8("XLEN")
  val XREAD = Buf.Utf8("XREAD")
  val XREADGROUP = Buf.Utf8("XREADGROUP")
  val XGROUP = Buf.Utf8("XGROUP")
  val XACK = Buf.Utf8("XACK")
  val XCLAIM = Buf.Utf8("XCLAIM")
  val XPENDING = Buf.Utf8("XPENDING")

  /**
   * Encodes a given [[Command]] as [[Buf]].
   */
  private[redis] def encode(c: Command): Buf = {
    val args = c.name +: c.body

    val header: Vector[Buf] = Vector(ARG_COUNT, Buf.Utf8(args.length.toString), EOL)

    val bufs = args.flatMap { arg =>
      Vector(ARG_SIZE, Buf.Utf8(arg.length.toString), EOL, arg, EOL)
    }

    Buf(header ++ bufs)
  }
}
