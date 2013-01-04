package com.twitter.finagle.redis.protocol.commands

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.Commands._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.redis.util.StringToChannelBuffer

case class BAdd(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer)
    extends StrictKeyCommand {
  def command = Commands.BADD
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.BADD, key, field, value))
}

object BAdd {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 3, "BADD")
    new BAdd(
      ChannelBuffers.wrappedBuffer(list(0)),
      ChannelBuffers.wrappedBuffer(list(1)),
      ChannelBuffers.wrappedBuffer(list(2)))
  }
}

object BRem {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(args.length > 2, "BREM requires a hash key and at least one field")
    new BRem(ChannelBuffers.wrappedBuffer(args(0)),
      args.drop(1).map(ChannelBuffers.wrappedBuffer(_)))
  }
}
case class BRem(key: ChannelBuffer, fields: Seq[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.BREM
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.BREM, key) ++ fields)
}

object BGet {
  def apply(args: Seq[Array[Byte]]) = {
    val list = trimList(args, 2, "BGET")
    new BGet(ChannelBuffers.wrappedBuffer(list(0)), ChannelBuffers.wrappedBuffer(list(1)))
  }
}

case class BGet(key: ChannelBuffer, field: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.BGET
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.BGET, key, field))
}

case class BCard(key: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.BCARD
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.BCARD, key))
}

object BCard {
  def apply(args: Seq[Array[Byte]]) = {
    RequireClientProtocol(!args.isEmpty, "BCARD requires at least one member")
    new BCard(ChannelBuffers.wrappedBuffer(args(0)))
  }
}

case class BRange(key: ChannelBuffer, startField: Option[ChannelBuffer], endField: Option[ChannelBuffer]) extends StrictKeyCommand {
  def command = Commands.BRANGE
  val request: Seq[ChannelBuffer] =
    if (startField.isEmpty && endField.isEmpty) Seq(key)
    else if (!startField.isEmpty && endField.isEmpty) Seq(key, StringToChannelBuffer("start"), startField.get)
    else if (startField.isEmpty && !endField.isEmpty) Seq(key, StringToChannelBuffer("end"), endField.get)
    else Seq(key, StringToChannelBuffer("startend"), startField.get, endField.get)
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.BRANGE) ++ request)
}
