package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.exp.SubscribeHandler
import com.twitter.io.Buf

abstract class SubscribeCommand extends Command {
  def handler: SubscribeHandler
}

case class PSubscribe(
    patterns: Seq[Buf],
    handler: SubscribeHandler)
  extends SubscribeCommand {

  def command: String = Commands.PSUBSCRIBE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.PSUBSCRIBE +: patterns)
}

case class PUnsubscribe(
    patterns: Seq[Buf],
    handler: SubscribeHandler)
  extends SubscribeCommand {

  def command: String = Commands.PUNSUBSCRIBE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.PUNSUBSCRIBE +: patterns)
}

case class Subscribe(
    channels: Seq[Buf],
    handler: SubscribeHandler)
  extends SubscribeCommand {

  def command: String = Commands.SUBSCRIBE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.SUBSCRIBE +: channels)
}

case class Unsubscribe(
    channels: Seq[Buf],
    handler: SubscribeHandler)
  extends SubscribeCommand {

  def command: String = Commands.UNSUBSCRIBE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.UNSUBSCRIBE +: channels)
}

case class Publish(key: Buf, message: Buf) extends StrictKeyCommand {
  def command: String = Commands.PUBLISH
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.PUBLISH, key, message))
}

case class PubSubChannels(pattern: Option[Buf]) extends PubSub(Buf.Utf8("CHANNELS"), pattern.toSeq)

case class PubSubNumSub(channels: Seq[Buf]) extends PubSub(Buf.Utf8("NUMSUB"), channels)

case object PubSubNumPat extends PubSub(Buf.Utf8("NUMPAT"), Seq.empty)

abstract class PubSub(sub: Buf, args: Seq[Buf]) extends Command {
  def command: String = Commands.PUBSUB
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.PUBSUB, sub) ++ args)
}
