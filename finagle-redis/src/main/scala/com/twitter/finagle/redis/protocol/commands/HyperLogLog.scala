package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class PFAdd(keyBuf: Buf, elements: Seq[Buf]) extends StrictKeyCommand {
  val command = Commands.PFADD
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PFADD, keyBuf) ++ elements)
}

object PFAdd {
  def apply(args: Seq[Array[Byte]]): PFAdd = args match {
    case head :: tail =>
      PFAdd(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))

    case _ =>
      throw ClientError("Invalid use of PFAdd")
  }
}

case class PFCount(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  override def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer.apply)

  val command = Commands.PFCOUNT

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.PFCOUNT +: keysBuf)
}

object PFCount {
  def apply(args: => Seq[Array[Byte]]): PFCount = PFCount(args.map(Buf.ByteArray.Owned.apply))
}

case class PFMerge(destKey: Buf, srcKeys: Seq[Buf]) extends Command {
  val command = Commands.PFMERGE

  RequireClientProtocol(srcKeys.size > 0, "srcKeys must not be empty")

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PFMERGE, destKey) ++ srcKeys)
}

object PFMerge {
  def apply(args: Seq[Array[Byte]]): PFMerge = args match {
    case head :: tail =>
      PFMerge(Buf.ByteArray.Owned(head), tail.map(Buf.ByteArray.Owned.apply))

    case _ =>
      throw ClientError("Invalid use of PFMerge")
  }
}
