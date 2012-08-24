package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

object HDel {
  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "HDEL requires a hash key and at least one field")
    new HDel(ChannelBuffers.wrappedBuffer(args(0)),
      args.drop(1).map(ChannelBuffers.wrappedBuffer(_)))
  }
}
case class HDel(key: ChannelBuffer, fields: List[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.HDEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HDEL, key) ::: fields)
}

object HGet {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 2, "HGET")
    new HGet(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}
case class HGet(key: ChannelBuffer, field: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HGET, key, field))
}

object HGetAll {
  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.nonEmpty, "HGETALL requires a key")
    new HGetAll(ChannelBuffers.wrappedBuffer(args(0)))
  }
}
case class HGetAll(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HGETALL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HGETALL, key))
}

object HKeys {
  def apply(keys: List[Array[Byte]]) = {
    new HKeys(ChannelBuffers.wrappedBuffer(keys.head))
  }
}
case class HKeys(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.HKEYS
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HKEYS, key))
}

object HMGet {
  def apply(args: List[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "HMGET requires a hash key and at least one field")
    new HMGet(ChannelBuffers.wrappedBuffer(args(0)),
      args.drop(1).map(ChannelBuffers.wrappedBuffer(_)))
  }
}
case class HMGet(key: ChannelBuffer, fields: List[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.HMGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HMGET, key) ::: fields)
}

object HSet {
  def apply(args: List[Array[Byte]]) = {
    val list = trimList(args, 3, "HSET")
    new HSet(
      ChannelBuffers.wrappedBuffer(list(0)),
      ChannelBuffers.wrappedBuffer(list(1)),
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}
case class HSet(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer)
  extends StrictKeyCommand {
  def command = Commands.HSET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(List(CommandBytes.HSET, key, field, value))
}
