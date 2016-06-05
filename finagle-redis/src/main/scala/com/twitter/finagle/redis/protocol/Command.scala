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
  val ZADD              = StringToChannelBuffer("ZADD")
  val ZCARD             = StringToChannelBuffer("ZCARD")
  val ZCOUNT            = StringToChannelBuffer("ZCOUNT")
  val ZINCRBY           = StringToChannelBuffer("ZINCRBY")
  val ZINTERSTORE       = StringToChannelBuffer("ZINTERSTORE")
  val ZRANGE            = StringToChannelBuffer("ZRANGE")
  val ZRANGEBYSCORE     = StringToChannelBuffer("ZRANGEBYSCORE")
  val ZRANK             = StringToChannelBuffer("ZRANK")
  val ZREM              = StringToChannelBuffer("ZREM")
  val ZREMRANGEBYRANK   = StringToChannelBuffer("ZREMRANGEBYRANK")
  val ZREMRANGEBYSCORE  = StringToChannelBuffer("ZREMRANGEBYSCORE")
  val ZREVRANGE         = StringToChannelBuffer("ZREVRANGE")
  val ZREVRANGEBYSCORE  = StringToChannelBuffer("ZREVRANGEBYSCORE")
  val ZREVRANK          = StringToChannelBuffer("ZREVRANK")
  val ZSCORE            = StringToChannelBuffer("ZSCORE")
  val ZUNIONSTORE       = StringToChannelBuffer("ZUNIONSTORE")

  // Btree Sorted Set
  // These are twitter-internal commands and will be removed eventually
  val BADD              = StringToChannelBuffer("BADD")
  val BCARD             = StringToChannelBuffer("BCARD")
  val BREM              = StringToChannelBuffer("BREM")
  val BGET              = StringToChannelBuffer("BGET")
  val BRANGE            = StringToChannelBuffer("BRANGE")

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
  val HDEL              = StringToChannelBuffer("HDEL")
  val HEXISTS           = StringToChannelBuffer("HEXISTS")
  val HGET              = StringToChannelBuffer("HGET")
  val HGETALL           = StringToChannelBuffer("HGETALL")
  val HINCRBY           = StringToChannelBuffer("HINCRBY")
  val HKEYS             = StringToChannelBuffer("HKEYS")
  val HMGET             = StringToChannelBuffer("HMGET")
  val HMSET             = StringToChannelBuffer("HMSET")
  val HSCAN             = StringToChannelBuffer("HSCAN")
  val HSET              = StringToChannelBuffer("HSET")
  val HSETNX            = StringToChannelBuffer("HSETNX")
  val HVALS             = StringToChannelBuffer("HVALS")

  // Lists
  val LLEN              = StringToChannelBuffer("LLEN")
  val LINDEX            = StringToChannelBuffer("LINDEX")
  val LINSERT           = StringToChannelBuffer("LINSERT")
  val LPOP              = StringToChannelBuffer("LPOP")
  val LPUSH             = StringToChannelBuffer("LPUSH")
  val LREM              = StringToChannelBuffer("LREM")
  val LSET              = StringToChannelBuffer("LSET")
  val LRANGE            = StringToChannelBuffer("LRANGE")
  val RPOP              = StringToChannelBuffer("RPOP")
  val RPUSH             = StringToChannelBuffer("RPUSH")
  val LTRIM             = StringToChannelBuffer("LTRIM")

  // Sets
  val SADD              = StringToChannelBuffer("SADD")
  val SMEMBERS          = StringToChannelBuffer("SMEMBERS")
  val SISMEMBER         = StringToChannelBuffer("SISMEMBER")
  val SCARD             = StringToChannelBuffer("SCARD")
  val SREM              = StringToChannelBuffer("SREM")
  val SPOP              = StringToChannelBuffer("SPOP")
  val SRANDMEMBER       = StringToChannelBuffer("SRANDMEMBER")
  val SINTER            = StringToChannelBuffer("SINTER")

  // Transactions
  val DISCARD           = StringToChannelBuffer("DISCARD")
  val EXEC              = StringToChannelBuffer("EXEC")
  val MULTI             = StringToChannelBuffer("MULTI")
  val UNWATCH           = StringToChannelBuffer("UNWATCH")
  val WATCH             = StringToBuf("WATCH")

  // HyperLogLogs
  val PFADD             = StringToChannelBuffer("PFADD")
  val PFCOUNT           = StringToChannelBuffer("PFCOUNT")
  val PFMERGE           = StringToChannelBuffer("PFMERGE")

  // PubSub
  val PUBLISH           = StringToChannelBuffer("PUBLISH")
  val SUBSCRIBE         = StringToChannelBuffer("SUBSCRIBE")
  val UNSUBSCRIBE       = StringToChannelBuffer("UNSUBSCRIBE")
  val PSUBSCRIBE        = StringToChannelBuffer("PSUBSCRIBE")
  val PUNSUBSCRIBE      = StringToChannelBuffer("PUNSUBSCRIBE")
  val PUBSUB            = StringToChannelBuffer("PUBSUB")
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
