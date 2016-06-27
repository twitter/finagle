package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.commands._
import com.twitter.finagle.redis.util._
import com.twitter.io.{Buf, Charsets}

object RequireClientProtocol extends ErrorConversion {
  override def getException(msg: String) = new ClientError(msg)
}

abstract class Command extends RedisMessage {
  def command: String
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
  val PATTERN           = "PATTERN"

  val commandMap: Map[String, Function1[List[Array[Byte]],Command]] = Map(
    // key commands
    DEL               -> {Del(_)},
    DUMP              -> {Dump(_)},
    EXISTS            -> {Exists(_)},
    EXPIRE            -> {Expire(_)},
    EXPIREAT          -> {ExpireAt(_)},
    KEYS              -> {Keys(_)},
    MOVE              -> {Move(_)},
    PERSIST           -> {Persist(_)},
    PEXPIRE           -> {PExpire(_)},
    PEXPIREAT         -> {PExpireAt(_)},
    PTTL              -> {PTtl(_)},
    RANDOMKEY         -> {_ => Randomkey()},
    RENAME            -> {Rename(_)},
    RENAMENX          -> {RenameNx(_)},
    SCAN              -> {Scan(_)},
    TTL               -> {Ttl(_)},
    TYPE              -> {Type(_)},

    // string commands
    APPEND            -> {Append(_)},
    BITCOUNT          -> {BitCount(_)},
    BITOP             -> {BitOp(_)},
    DECR              -> {Decr(_)},
    DECRBY            -> {DecrBy(_)},
    GET               -> {Get(_)},
    GETBIT            -> {GetBit(_)},
    GETRANGE          -> {GetRange(_)},
    GETSET            -> {GetSet(_)},
    INCR              -> {Incr(_)},
    INCRBY            -> {IncrBy(_)},
    MGET              -> {MGet(_)},
    MSET              -> {MSet(_)},
    MSETNX            -> {MSetNx(_)},
    PSETEX            -> {PSetEx(_)},
    SET               -> {Set(_)},
    SETBIT            -> {SetBit(_)},
    SETEX             -> {SetEx(_)},
    SETNX             -> {SetNx(_)},
    SETRANGE          -> {SetRange(_)},
    STRLEN            -> {Strlen(_)},

    // sorted sets
    ZADD              -> {ZAdd(_)},
    ZCARD             -> {ZCard(_)},
    ZCOUNT            -> {ZCount(_)},
    ZINCRBY           -> {ZIncrBy(_)},
    ZINTERSTORE       -> {ZInterStore(_)},
    ZRANGE            -> {ZRange(_)},
    ZRANGEBYSCORE     -> {ZRangeByScore(_)},
    ZRANK             -> {ZRank(_)},
    ZREM              -> {ZRem(_)},
    ZREMRANGEBYRANK   -> {ZRemRangeByRank(_)},
    ZREMRANGEBYSCORE  -> {ZRemRangeByScore(_)},
    ZREVRANGE         -> {ZRevRange(_)},
    ZREVRANGEBYSCORE  -> {ZRevRangeByScore(_)},
    ZREVRANK          -> {ZRevRank(_)},
    ZSCORE            -> {ZScore(_)},
    ZUNIONSTORE       -> {ZUnionStore(_)},

    // Btree Sorted Set
    // These are twitter-internal commands and will be removed eventually
    BADD              -> {BAdd(_)},
    BCARD             -> {BCard(_)},
    BREM              -> {BRem(_)},
    BGET              -> {BGet(_)},

    // Topology
    // These are twitter-internal commands and will be removed eventually
    TOPOLOGYADD       -> {TopologyAdd(_)},
    TOPOLOGYGET       -> {TopologyGet(_)},
    TOPOLOGYDELETE    -> {TopologyDelete(_)},

    // miscellaneous
    PING              -> {_ => Ping},
    FLUSHALL          -> {_ => FlushAll},
    FLUSHDB           -> {_ => FlushDB},
    SELECT            -> {Select(_)},
    AUTH              -> {Auth(_)},
    INFO              -> {Info(_)},
    QUIT              -> {_ => Quit},
    SLAVEOF           -> {SlaveOf(_)},
    CONFIG            -> {Config(_)},
    SENTINEL          -> {Sentinel.fromBytes(_)},

    // hash sets
    HDEL              -> {HDel(_)},
    HEXISTS           -> {HExists(_)},
    HGET              -> {HGet(_)},
    HGETALL           -> {HGetAll(_)},
    HINCRBY           -> {HIncrBy(_)},
    HKEYS             -> {HKeys(_)},
    HLEN              -> {HLen(_)},
    HMGET             -> {HMGet(_)},
    HMSET             -> {HMSet(_)},
    HSCAN             -> {HScan(_)},
    HSET              -> {HSet(_)},
    HSETNX            -> {HSetNx(_)},
    HVALS             -> {HVals(_)},

    // Lists
    LLEN              -> {LLen(_)},
    LINDEX            -> {LIndex(_)},
    LINSERT           -> {LInsert(_)},
    LPOP              -> {LPop(_)},
    LPUSH             -> {LPush(_)},
    LREM              -> {LRem(_)},
    LSET              -> {LSet(_)},
    LRANGE            -> {LRange(_)},
    RPUSH             -> {RPush(_)},
    RPOP              -> {RPop(_)},
    LTRIM             -> {LTrim(_)},

    // Sets
    SADD              -> {SAdd(_)},
    SMEMBERS          -> {SMembers(_)},
    SISMEMBER         -> {SIsMember(_)},
    SCARD             -> {SCard(_)},
    SREM              -> {SRem(_)},
    SPOP              -> {SPop(_)},
    SRANDMEMBER       -> {SRandMember(_)},
    SINTER            -> {SInter(_)},

    // transactions
    DISCARD           -> {_ => Discard},
    EXEC              -> {_ => Exec},
    MULTI             -> {_ => Multi},
    UNWATCH           -> {_ => UnWatch},
    WATCH             -> {Watch(_)},

    // HyperLogLogs
    PFADD             -> {PFAdd(_)},
    PFCOUNT           -> {PFCount(_)},
    PFMERGE           -> {PFMerge(_)},

    // PubSub
    PUBLISH           -> {Publish(_)},
    PUBSUB            -> {PubSub(_)}

    // TODO: add Scripts command map
  )

  def doMatch(cmd: String, args: List[Array[Byte]]) = commandMap.get(cmd.toUpperCase).map {
    _(args)
  }.getOrElse(throw ClientError("Unsupported command: " + cmd))

  def trimList[T](list: Seq[T], count: Int, from: String = ""): Seq[T] = {
    RequireClientProtocol(list != null, "%s Empty list found".format(from))
    RequireClientProtocol(
      list.length == count,
      "%s Expected %d elements, found %d".format(from, count, list.length))
    val newList = list.take(count)
    newList.foreach { item => RequireClientProtocol(item != null, "Found empty item in list") }
    newList
  }
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
  val PING              = StringToChannelBuffer("PING")
  val FLUSHALL          = StringToChannelBuffer("FLUSHALL")
  val FLUSHDB           = StringToChannelBuffer("FLUSHDB")
  val SELECT            = StringToChannelBuffer("SELECT")
  val AUTH              = StringToChannelBuffer("AUTH")
  val INFO              = StringToChannelBuffer("INFO")
  val QUIT              = StringToChannelBuffer("QUIT")
  val SLAVEOF           = StringToChannelBuffer("SLAVEOF")
  val CONFIG            = StringToChannelBuffer("CONFIG")
  val SENTINEL          = StringToChannelBuffer("SENTINEL")

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
  val DISCARD           = StringToChannelBuffer("DISCARD")
  val EXEC              = StringToChannelBuffer("EXEC")
  val MULTI             = StringToChannelBuffer("MULTI")
  val UNWATCH           = StringToChannelBuffer("UNWATCH")
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
  val PATTERN           = StringToBuf("PATTERN")
}


class CommandCodec extends UnifiedProtocolCodec {

  import RedisCodec._
  import com.twitter.finagle.redis.naggati.Encoder
  import com.twitter.finagle.redis.naggati.Stages._
  import com.twitter.logging.Logger

  val log = Logger(getClass)

  val encode = new Encoder[Command] {
    def encode(obj: Command) = Some(obj.toChannelBuffer)
  }

  val decode = readBytes(1) { bytes =>
    bytes(0) match {
      case ARG_COUNT_MARKER =>
        val doneFn = { lines => commandDecode(lines) }
        RequireClientProtocol.safe {
          readLine { line => decodeUnifiedFormat(NumberFormat.toLong(line), doneFn) }
        }
      case b: Byte =>
        decodeInlineRequest(b.asInstanceOf[Char])
    }
  }

  def decodeInlineRequest(c: Char) = readLine { line =>
    val listOfArrays = (c + line).split(' ').toList.map {
      args => args.getBytes(Charsets.Utf8)
    }
    val cmd = commandDecode(listOfArrays)
    emit(cmd)
  }

  def commandDecode(lines: List[Array[Byte]]): RedisMessage = {
    RequireClientProtocol(lines != null && lines.length > 0, "Invalid client command protocol")
    val cmd = BytesToString(lines.head)
    val args = lines.tail
    try {
      Commands.doMatch(cmd, args)
    } catch {
      case e: ClientError => throw e
      case t: Throwable =>
        log.warning(t, "Unhandled exception %s(%s)".format(t.getClass.toString, t.getMessage))
        throw new ClientError(t.getMessage)
    }
  }
}
