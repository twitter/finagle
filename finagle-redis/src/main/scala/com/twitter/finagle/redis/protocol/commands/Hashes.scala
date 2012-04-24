package com.twitter.finagle.redis.protocol

import Commands.trimList
import com.twitter.finagle.redis.util._

object HDel {
  def apply(key: String, fields: List[Array[Byte]]) = {
    RequireClientProtocol(fields.nonEmpty, "HDEL requires a hash key and at least one field")
    val list = BytesToString.fromList(fields)
    new HDel(key, list)
  }

  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "HDEL requires a hash key and at least one field")
    val key = BytesToString(args(0))
    val fields = BytesToString.fromList(args.drop(1))
    new HDel(key, fields)
  }
}

case class HDel(key: String, fields: Seq[String]) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.HDEL, key) ::: fields.toList)
}

object HGet {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "HGET")
    new HGet(list(0), list(1))
  }
}

case class HGet(key: Array[Byte], field: Array[Byte]) extends StrictByteKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(Commands.HGET.getBytes, key, field))
}

object HGetAll {
  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.nonEmpty, "HGETALL requires a key")
    new HGetAll(args(0))
  }
}

case class HGetAll(key: Array[Byte]) extends StrictByteKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(Commands.HGETALL.getBytes, key))
}

object HMGet {
  def apply(key: String, fields: List[Array[Byte]]) = {
    RequireClientProtocol(fields.nonEmpty, "HMGET requires a hash key and at least one field")
    val list = BytesToString.fromList(fields)
    new HMGet(key, list)
  }

  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "HMGET requires a hash key and at least one field")
    val key = BytesToString(args(0))
    val fields = BytesToString.fromList(args.drop(1))
    new HMGet(key, fields)
  }
}

case class HMGet(key: String, fields: Seq[String]) extends StrictKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toInlineFormat(List(Commands.HMGET, key) ::: fields.toList)
}

object HSet {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 3, "HSET")
    new HSet(list(0), list(1), list(2))
  }
}

case class HSet(key: Array[Byte], field: Array[Byte], value: Array[Byte])
  extends StrictByteKeyCommand {
  override def toChannelBuffer =
    RedisCodec.toUnifiedFormat(List(Commands.HSET.getBytes, key, field, value))
}