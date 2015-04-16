package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

case class PFAdd(key: ChannelBuffer, elements: Seq[ChannelBuffer]) extends StrictKeyCommand {
  val command = Commands.PFADD

  override def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PFADD, key) ++ elements)
}

object PFAdd {
  def apply(args: Seq[Array[Byte]]): PFAdd = args match {
    case head :: tail =>
      PFAdd(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)

    case _ =>
      throw ClientError("Invalid use of PFAdd")
  }
}

case class PFCount(keys: Seq[ChannelBuffer]) extends StrictKeysCommand {
  val command = Commands.PFCOUNT

  override def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.PFCOUNT +: keys)
}

object PFCount {
  def apply(args: => Seq[Array[Byte]]): PFCount = PFCount(args map ChannelBuffers.wrappedBuffer)
}

case class PFMerge(destKey: ChannelBuffer, srcKeys: Seq[ChannelBuffer]) extends Command {
  val command = Commands.PFMERGE

  RequireClientProtocol(srcKeys.size > 0, "srcKeys must not be empty")

  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PFMERGE, destKey) ++ srcKeys)
}

object PFMerge {
  def apply(args: Seq[Array[Byte]]): PFMerge = args match {
    case head :: tail =>
      PFMerge(ChannelBuffers.wrappedBuffer(head), tail map ChannelBuffers.wrappedBuffer)

    case _ =>
      throw ClientError("Invalid use of PFMerge")
  }
}
