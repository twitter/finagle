package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.commands._
import com.twitter.finagle.redis.util._
import com.twitter.io.Charsets

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
  val BADD              = "BADD"
  val BCARD             = "BCARD"
  val BREM              = "BREM"
  val BGET              = "BGET"
  val BRANGE            = "BRANGE"

  // Miscellaneous
  val FLUSHALL          = "FLUSHALL"
  val FLUSHDB           = "FLUSHDB"
  val SELECT            = "SELECT"
  val AUTH              = "AUTH"
  val INFO              = "INFO"
  val QUIT              = "QUIT"
  val SLAVEOF           = "SLAVEOF"
  val CONFIG            = "CONFIG"

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
    BADD              -> {BAdd(_)},
    BCARD             -> {BCard(_)},
    BREM              -> {BRem(_)},
    BGET              -> {BGet(_)},

    // miscellaneous
    FLUSHALL          -> {_ => FlushAll},
    FLUSHDB           -> {_ => FlushDB},
    SELECT            -> {Select(_)},
    AUTH              -> {Auth(_)},
    INFO              -> {Info(_)},
    QUIT              -> {_ => Quit},
    SLAVEOF           -> {SlaveOf(_)},
    CONFIG            -> {Config(_)},

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
    PFMERGE           -> {PFMerge(_)}

  )

  def doMatch(cmd: String, args: List[Array[Byte]]) = commandMap.get(cmd.toUpperCase).map {
    _(args)
  }.getOrElse(throw ClientError("Unsupported command: " + cmd))

  def trimList(list: Seq[Array[Byte]], count: Int, from: String = "") = {
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
  val DEL               = StringToChannelBuffer("DEL")
  val DUMP              = StringToChannelBuffer("DUMP")
  val EXISTS            = StringToChannelBuffer("EXISTS")
  val EXPIRE            = StringToChannelBuffer("EXPIRE")
  val EXPIREAT          = StringToChannelBuffer("EXPIREAT")
  val KEYS              = StringToChannelBuffer("KEYS")
  val MOVE              = StringToChannelBuffer("MOVE")
  val PERSIST           = StringToChannelBuffer("PERSIST")
  val PEXPIRE           = StringToChannelBuffer("PEXPIRE")
  val PEXPIREAT         = StringToChannelBuffer("PEXPIREAT")
  val PTTL              = StringToChannelBuffer("PTTL")
  val RANDOMKEY         = StringToChannelBuffer("RANDOMKEY")
  val RENAME            = StringToChannelBuffer("RENAME")
  val RENAMENX          = StringToChannelBuffer("RENAMENX")
  val SCAN              = StringToChannelBuffer("SCAN")
  val TTL               = StringToChannelBuffer("TTL")
  val TYPE              = StringToChannelBuffer("TYPE")

  // String Commands
  val APPEND            = StringToChannelBuffer("APPEND")
  val BITCOUNT          = StringToChannelBuffer("BITCOUNT")
  val BITOP             = StringToChannelBuffer("BITOP")
  val DECR              = StringToChannelBuffer("DECR")
  val DECRBY            = StringToChannelBuffer("DECRBY")
  val GET               = StringToChannelBuffer("GET")
  val GETBIT            = StringToChannelBuffer("GETBIT")
  val GETRANGE          = StringToChannelBuffer("GETRANGE")
  val GETSET            = StringToChannelBuffer("GETSET")
  val INCR              = StringToChannelBuffer("INCR")
  val INCRBY            = StringToChannelBuffer("INCRBY")
  val MGET              = StringToChannelBuffer("MGET")
  val MSET              = StringToChannelBuffer("MSET")
  val MSETNX            = StringToChannelBuffer("MSETNX")
  val PSETEX            = StringToChannelBuffer("PSETEX")
  val SET               = StringToChannelBuffer("SET")
  val SETBIT            = StringToChannelBuffer("SETBIT")
  val SETEX             = StringToChannelBuffer("SETEX")
  val SETNX             = StringToChannelBuffer("SETNX")
  val SETRANGE          = StringToChannelBuffer("SETRANGE")
  val STRLEN            = StringToChannelBuffer("STRLEN")

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
  val BADD              = StringToChannelBuffer("BADD")
  val BCARD             = StringToChannelBuffer("BCARD")
  val BREM              = StringToChannelBuffer("BREM")
  val BGET              = StringToChannelBuffer("BGET")
  val BRANGE            = StringToChannelBuffer("BRANGE")

  // Miscellaneous
  val FLUSHALL          = StringToChannelBuffer("FLUSHALL")
  val FLUSHDB           = StringToChannelBuffer("FLUSHDB")
  val SELECT            = StringToChannelBuffer("SELECT")
  val AUTH              = StringToChannelBuffer("AUTH")
  val INFO              = StringToChannelBuffer("INFO")
  val QUIT              = StringToChannelBuffer("QUIT")
  val SLAVEOF           = StringToChannelBuffer("SLAVEOF")
  val CONFIG            = StringToChannelBuffer("CONFIG")

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
  val WATCH             = StringToChannelBuffer("WATCH")

  // HyperLogLogs
  val PFADD             = StringToChannelBuffer("PFADD")
  val PFCOUNT           = StringToChannelBuffer("PFCOUNT")
  val PFMERGE           = StringToChannelBuffer("PFMERGE")
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

  def commandDecode(lines: List[Array[Byte]]): Command = {
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
