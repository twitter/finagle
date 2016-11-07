package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf

object RequireClientProtocol extends ErrorConversion {
  override def getException(msg: String) = new ClientError(msg)
}

abstract class Command extends RedisMessage {
  def command: String
  def toBuf: Buf
}

object Commands {
  // Key Commands
  val DEL       = "DEL"
  val DUMP      = "DUMP"
  val EXISTS    = "EXISTS"
  val EXPIRE    = "EXPIRE"
  val EXPIREAT  = "EXPIREAT"
  val KEYS      = "KEYS"
  val MOVE      = "MOVE"
  val PERSIST   = "PERSIST"
  val PEXPIRE   = "PEXPIRE"
  val PEXPIREAT = "PEXPIREAT"
  val PTTL      = "PTTL"
  val RANDOMKEY = "RANDOMKEY"
  val RENAME    = "RENAME"
  val RENAMENX  = "RENAMENX"
  val SCAN      = "SCAN"
  val TTL       = "TTL"
  val TYPE      = "TYPE"

  // String Commands
  val APPEND    = "APPEND"
  val BITCOUNT  = "BITCOUNT"
  val BITOP     = "BITOP"
  val DECR      = "DECR"
  val DECRBY    = "DECRBY"
  val GET       = "GET"
  val GETBIT    = "GETBIT"
  val GETRANGE  = "GETRANGE"
  val GETSET    = "GETSET"
  val INCR      = "INCR"
  val INCRBY    = "INCRBY"
  val MGET      = "MGET"
  val MSET      = "MSET"
  val MSETNX    = "MSETNX"
  val PSETEX    = "PSETEX"
  val SET       = "SET"
  val SETBIT    = "SETBIT"
  val SETEX     = "SETEX"
  val SETNX     = "SETNX"
  val SETRANGE  = "SETRANGE"
  val STRLEN    = "STRLEN"

  // Sorted Sets
  val ZADD              = "ZADD"
  val ZCARD             = "ZCARD"
  val ZCOUNT            = "ZCOUNT"
  val ZINCRBY           = "ZINCRBY"
  val ZINTERSTORE       = "ZINTERSTORE"
  val ZRANGE            = "ZRANGE"
  val ZRANGEBYSCORE     = "ZRANGEBYSCORE"
  val ZRANK             = "ZRANK"
  val ZREM              = "ZREM"
  val ZREMRANGEBYRANK   = "ZREMRANGEBYRANK"
  val ZREMRANGEBYSCORE  = "ZREMRANGEBYSCORE"
  val ZREVRANGE         = "ZREVRANGE"
  val ZREVRANGEBYSCORE  = "ZREVRANGEBYSCORE"
  val ZREVRANK          = "ZREVRANK"
  val ZSCORE            = "ZSCORE"
  val ZUNIONSTORE       = "ZUNIONSTORE"

  // Btree Sorted Set
  // These are twitter-internal commands and will be removed eventually
  val BADD              = "BADD"
  val BCARD             = "BCARD"
  val BREM              = "BREM"
  val BGET              = "BGET"
  val BRANGE            = "BRANGE"

  // Topology
  // These are twitter-internal commands and will be removed eventually
  val TOPOLOGYADD       = "TOPOLOGYADD"
  val TOPOLOGYGET       = "TOPOLOGYGET"
  val TOPOLOGYDELETE    = "TOPOLOGYDELETE"

  // Miscellaneous
  val PING              = "PING"
  val FLUSHALL          = "FLUSHALL"
  val FLUSHDB           = "FLUSHDB"
  val SELECT            = "SELECT"
  val AUTH              = "AUTH"
  val INFO              = "INFO"
  val QUIT              = "QUIT"
  val SLAVEOF           = "SLAVEOF"
  val CONFIG            = "CONFIG"
  val SENTINEL          = "SENTINEL"

  // Hash Sets
  val HDEL              = "HDEL"
  val HEXISTS           = "HEXISTS"
  val HGET              = "HGET"
  val HGETALL           = "HGETALL"
  val HINCRBY           = "HINCRBY"
  val HKEYS             = "HKEYS"
  val HLEN              = "HLEN"
  val HMGET             = "HMGET"
  val HMSET             = "HMSET"
  val HSCAN             = "HSCAN"
  val HSET              = "HSET"
  val HSETNX            = "HSETNX"
  val HVALS             = "HVALS"

  // Lists
  val LLEN              = "LLEN"
  val LINDEX            = "LINDEX"
  val LINSERT           = "LINSERT"
  val LPOP              = "LPOP"
  val LPUSH             = "LPUSH"
  val LREM              = "LREM"
  val LSET              = "LSET"
  val LRANGE            = "LRANGE"
  val RPOP              = "RPOP"
  val RPUSH             = "RPUSH"
  val LTRIM             = "LTRIM"

  // Sets
  val SADD              = "SADD"
  val SMEMBERS          = "SMEMBERS"
  val SISMEMBER         = "SISMEMBER"
  val SCARD             = "SCARD"
  val SREM              = "SREM"
  val SPOP              = "SPOP"
  val SRANDMEMBER       = "SRANDMEMBER"
  val SINTER            = "SINTER"

  // Transactions
  val DISCARD           = "DISCARD"
  val EXEC              = "EXEC"
  val MULTI             = "MULTI"
  val UNWATCH           = "UNWATCH"
  val WATCH             = "WATCH"

  // HyperLogLogs
  val PFADD             = "PFADD"
  val PFCOUNT           = "PFCOUNT"
  val PFMERGE           = "PFMERGE"

  // PubSub
  val PUBLISH           = "PUBLISH"
  val SUBSCRIBE         = "SUBSCRIBE"
  val UNSUBSCRIBE       = "UNSUBSCRIBE"
  val PSUBSCRIBE        = "PSUBSCRIBE"
  val PUNSUBSCRIBE      = "PUNSUBSCRIBE"
  val PUBSUB            = "PUBSUB"

  // Scripts
  val EVAL              = "EVAL"
  val EVALSHA           = "EVALSHA"
  val SCRIPT            = "SCRIPT"
  val FLUSH             = "FLUSH"
  val LOAD              = "LOAD"
  val SCRIPTEXISTS      = "SCRIPT EXISTS"
  val SCRIPTFLUSH       = "SCRIPT FLUSH"
  val SCRIPTLOAD        = "SCRIPT LOAD"

  // Command Arguments
  val WITHSCORES        = "WITHSCORES"
  val LIMIT             = "LIMIT"
  val WEIGHTS           = "WEIGHTS"
  val AGGREGATE         = "AGGREGATE"
  val COUNT             = "COUNT"
  val MATCH             = "MATCH"
}

object CommandBytes {
  // Key Commands
  val DEL: Buf               = StringToBuf("DEL")
  val DUMP: Buf              = StringToBuf("DUMP")
  val EXISTS: Buf            = StringToBuf("EXISTS")
  val EXPIRE: Buf            = StringToBuf("EXPIRE")
  val EXPIREAT: Buf          = StringToBuf("EXPIREAT")
  val KEYS: Buf              = StringToBuf("KEYS")
  val MOVE: Buf              = StringToBuf("MOVE")
  val PERSIST: Buf           = StringToBuf("PERSIST")
  val PEXPIRE: Buf           = StringToBuf("PEXPIRE")
  val PEXPIREAT: Buf         = StringToBuf("PEXPIREAT")
  val PTTL: Buf              = StringToBuf("PTTL")
  val RANDOMKEY: Buf         = StringToBuf("RANDOMKEY")
  val RENAME: Buf            = StringToBuf("RENAME")
  val RENAMENX: Buf          = StringToBuf("RENAMENX")
  val SCAN: Buf              = StringToBuf("SCAN")
  val TTL: Buf               = StringToBuf("TTL")
  val TYPE: Buf              = StringToBuf("TYPE")

  // String Commands
  val APPEND            = StringToBuf("APPEND")
  val BITCOUNT          = StringToBuf("BITCOUNT")
  val BITOP             = StringToBuf("BITOP")
  val DECR              = StringToBuf("DECR")
  val DECRBY            = StringToBuf("DECRBY")
  val GET               = StringToBuf("GET")
  val GETBIT            = StringToBuf("GETBIT")
  val GETRANGE          = StringToBuf("GETRANGE")
  val GETSET            = StringToBuf("GETSET")
  val INCR              = StringToBuf("INCR")
  val INCRBY            = StringToBuf("INCRBY")
  val MGET              = StringToBuf("MGET")
  val MSET              = StringToBuf("MSET")
  val MSETNX            = StringToBuf("MSETNX")
  val PSETEX            = StringToBuf("PSETEX")
  val SET               = StringToBuf("SET")
  val SETBIT            = StringToBuf("SETBIT")
  val SETEX             = StringToBuf("SETEX")
  val SETNX             = StringToBuf("SETNX")
  val SETRANGE          = StringToBuf("SETRANGE")
  val STRLEN            = StringToBuf("STRLEN")

  // Sorted Sets
  val ZADD              = StringToBuf("ZADD")
  val ZCARD             = StringToBuf("ZCARD")
  val ZCOUNT            = StringToBuf("ZCOUNT")
  val ZINCRBY           = StringToBuf("ZINCRBY")
  val ZINTERSTORE       = StringToBuf("ZINTERSTORE")
  val ZRANGE            = StringToBuf("ZRANGE")
  val ZRANGEBYSCORE     = StringToBuf("ZRANGEBYSCORE")
  val ZRANK             = StringToBuf("ZRANK")
  val ZREM              = StringToBuf("ZREM")
  val ZREMRANGEBYRANK   = StringToBuf("ZREMRANGEBYRANK")
  val ZREMRANGEBYSCORE  = StringToBuf("ZREMRANGEBYSCORE")
  val ZREVRANGE         = StringToBuf("ZREVRANGE")
  val ZREVRANGEBYSCORE  = StringToBuf("ZREVRANGEBYSCORE")
  val ZREVRANK          = StringToBuf("ZREVRANK")
  val ZSCORE            = StringToBuf("ZSCORE")
  val ZUNIONSTORE       = StringToBuf("ZUNIONSTORE")

  // Btree Sorted Set
  // These are twitter-internal commands and will be removed eventually
  val BADD              = StringToBuf("BADD")
  val BCARD             = StringToBuf("BCARD")
  val BREM              = StringToBuf("BREM")
  val BGET              = StringToBuf("BGET")
  val BRANGE            = StringToBuf("BRANGE")

  // Topology
  // These are twitter-internal commands and will be removed eventually
  val TOPOLOGYADD      = StringToBuf("TOPOLOGYADD")
  val TOPOLOGYGET      = StringToBuf("TOPOLOGYGET")
  val TOPOLOGYDELETE   = StringToBuf("TOPOLOGYDELETE")

  // Miscellaneous
  val PING              = StringToBuf("PING")
  val FLUSHALL          = StringToBuf("FLUSHALL")
  val FLUSHDB           = StringToBuf("FLUSHDB")
  val SELECT            = StringToBuf("SELECT")
  val AUTH              = StringToBuf("AUTH")
  val INFO              = StringToBuf("INFO")
  val QUIT              = StringToBuf("QUIT")
  val SLAVEOF           = StringToBuf("SLAVEOF")
  val CONFIG            = StringToBuf("CONFIG")
  val SENTINEL          = StringToBuf("SENTINEL")

  // Hash Sets
  val HDEL              = StringToBuf("HDEL")
  val HEXISTS           = StringToBuf("HEXISTS")
  val HGET              = StringToBuf("HGET")
  val HGETALL           = StringToBuf("HGETALL")
  val HINCRBY           = StringToBuf("HINCRBY")
  val HKEYS             = StringToBuf("HKEYS")
  val HLEN              = StringToBuf("HLEN")
  val HMGET             = StringToBuf("HMGET")
  val HMSET             = StringToBuf("HMSET")
  val HSCAN             = StringToBuf("HSCAN")
  val HSET              = StringToBuf("HSET")
  val HSETNX            = StringToBuf("HSETNX")
  val HVALS             = StringToBuf("HVALS")

  // Lists
  val LLEN              = StringToBuf("LLEN")
  val LINDEX            = StringToBuf("LINDEX")
  val LINSERT           = StringToBuf("LINSERT")
  val LPOP              = StringToBuf("LPOP")
  val LPUSH             = StringToBuf("LPUSH")
  val LREM              = StringToBuf("LREM")
  val LSET              = StringToBuf("LSET")
  val LRANGE            = StringToBuf("LRANGE")
  val RPOP              = StringToBuf("RPOP")
  val RPUSH             = StringToBuf("RPUSH")
  val LTRIM             = StringToBuf("LTRIM")

  // Sets
  val SADD              = StringToBuf("SADD")
  val SMEMBERS          = StringToBuf("SMEMBERS")
  val SISMEMBER         = StringToBuf("SISMEMBER")
  val SCARD             = StringToBuf("SCARD")
  val SREM              = StringToBuf("SREM")
  val SPOP              = StringToBuf("SPOP")
  val SRANDMEMBER       = StringToBuf("SRANDMEMBER")
  val SINTER            = StringToBuf("SINTER")

  // Transactions
  val DISCARD           = StringToBuf("DISCARD")
  val EXEC              = StringToBuf("EXEC")
  val MULTI             = StringToBuf("MULTI")
  val UNWATCH           = StringToBuf("UNWATCH")
  val WATCH             = StringToBuf("WATCH")

  // HyperLogLogs
  val PFADD             = StringToBuf("PFADD")
  val PFCOUNT           = StringToBuf("PFCOUNT")
  val PFMERGE           = StringToBuf("PFMERGE")

  // PubSub
  val PUBLISH           = StringToBuf("PUBLISH")
  val SUBSCRIBE         = StringToBuf("SUBSCRIBE")
  val UNSUBSCRIBE       = StringToBuf("UNSUBSCRIBE")
  val PSUBSCRIBE        = StringToBuf("PSUBSCRIBE")
  val PUNSUBSCRIBE      = StringToBuf("PUNSUBSCRIBE")
  val PUBSUB            = StringToBuf("PUBSUB")

  // Scripts
  val EVAL              = StringToBuf("EVAL")
  val EVALSHA           = StringToBuf("EVALSHA")
  val SCRIPT            = StringToBuf("SCRIPT")
  val FLUSH             = StringToBuf("FLUSH")
  val LOAD              = StringToBuf("LOAD")
  val SCRIPTEXISTS      = StringToBuf("SCRIPT EXISTS")
  val SCRIPTFLUSH       = StringToBuf("SCRIPT FLUSH")
  val SCRIPTLOAD        = StringToBuf("SCRIPT LOAD")
  // SCRIPT EXISTS, SCRIPT LOAD, SCRIPT FLUSH are subcommands
  // which must not be transmitted as a whole ChannelBuffer

  // Command Arguments
  val WITHSCORES        = StringToBuf("WITHSCORES")
  val LIMIT             = StringToBuf("LIMIT")
  val WEIGHTS           = StringToBuf("WEIGHTS")
  val AGGREGATE         = StringToBuf("AGGREGATE")
  val COUNT             = StringToBuf("COUNT")
  val MATCH             = StringToBuf("MATCH")
}
