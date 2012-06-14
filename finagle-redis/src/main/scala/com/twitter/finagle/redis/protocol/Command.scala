package com.twitter.finagle.redis
package protocol

import util._

object RequireClientProtocol extends ErrorConversion {
  override def getException(msg: String) = new ClientError(msg)
}

abstract class Command extends RedisMessage {
  val command: String
}

object Commands {
  // Key Commands
  val DEL       = "DEL"
  val EXISTS    = "EXISTS"
  val EXPIRE    = "EXPIRE"
  val EXPIREAT  = "EXPIREAT"
  val KEYS      = "KEYS"
  val PERSIST   = "PERSIST"
  val RANDOMKEY = "RANDOMKEY"
  val RENAME    = "RENAME"
  val RENAMENX  = "RENAMENX"
  val TTL       = "TTL"
  val TYPE      = "TYPE"

  // String Commands
  val APPEND    = "APPEND"
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

  // Miscellaneous
  val FLUSHDB           = "FLUSHDB"
  val SELECT            = "SELECT"
  val AUTH              = "AUTH"
  val QUIT              = "QUIT"

  // Hash Sets
  val HDEL              = "HDEL"
  val HGET              = "HGET"
  val HGETALL           = "HGETALL"
  val HMGET             = "HMGET"
  val HSET              = "HSET"

  val commandMap: Map[String,Function1[List[Array[Byte]],Command]] = Map(
    // key commands
    DEL               -> {args => Del(BytesToString.fromList(args))},
    EXISTS            -> {Exists(_)},
    EXPIRE            -> {Expire(_)},
    EXPIREAT          -> {ExpireAt(_)},
    KEYS              -> {Keys(_)},
    PERSIST           -> {Persist(_)},
    RANDOMKEY         -> {_ => Randomkey()},
    RENAME            -> {Rename(_)},
    RENAMENX          -> {RenameNx(_)},
    TTL               -> {Ttl(_)},
    TYPE              -> {Type(_)},

    // string commands
    APPEND            -> {Append(_)},
    DECR              -> {Decr(_)},
    DECRBY            -> {DecrBy(_)},
    GET               -> {Get(_)},
    GETBIT            -> {GetBit(_)},
    GETRANGE          -> {GetRange(_)},
    GETSET            -> {GetSet(_)},
    INCR              -> {Incr(_)},
    INCRBY            -> {IncrBy(_)},
    MGET              -> {args => MGet(BytesToString.fromList(args))},
    MSET              -> {MSet(_)},
    MSETNX            -> {MSetNx(_)},
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

    // miscellaneous
    FLUSHDB           -> {_ => FlushDB()},
    SELECT            -> {Select(_)},
    AUTH              -> {Auth(_)},
    QUIT              -> {_ => Quit()},

    // hash sets
    HDEL              -> {HDel(_)},
    HGET              -> {HGet(_)},
    HGETALL           -> {HGetAll(_)},
    HMGET             -> {HMGet(_)},
    HSET              -> {HSet(_)}

  )

  def doMatch(cmd: String, args: List[Array[Byte]]) = commandMap.get(cmd).map {
    _(args)
  }.getOrElse(throw ClientError("Unsupported command: " + cmd))

  def trimList(list: List[Array[Byte]], count: Int, from: String = "") = {
    RequireClientProtocol(list != null, "%s Empty list found".format(from))
    RequireClientProtocol(
      list.length == count,
      "%s Expected %d elements, found %d".format(from, count, list.length))
    val newList = list.take(count)
    newList.foreach { item => RequireClientProtocol(item != null, "Found empty item in list") }
    newList
  }
}

class CommandCodec extends UnifiedProtocolCodec {
  import com.twitter.finagle.redis.naggati.{Emit, Encoder, NextStep}
  import com.twitter.finagle.redis.naggati.Stages._
  import RedisCodec._
  import com.twitter.logging.Logger

  val log = Logger(getClass)

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

  val encode = new Encoder[Command] {
    def encode(obj: Command) = Some(obj.toChannelBuffer)
  }

  def decodeInlineRequest(c: Char) = readLine { line =>
    val listOfArrays = (c + line).split(' ').toList.map { args => args.getBytes("UTF-8") }
    val cmd = commandDecode(listOfArrays)
    emit(cmd)
  }

  def commandDecode(lines: List[Array[Byte]]): Command = {
    RequireClientProtocol(lines != null && lines.length > 0, "Invalid client command protocol")
    val cmd = new String(lines.head)
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
