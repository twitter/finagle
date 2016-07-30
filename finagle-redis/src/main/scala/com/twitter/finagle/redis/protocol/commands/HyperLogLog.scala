package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class PFAdd(keyBuf: Buf, elements: Seq[Buf]) extends StrictKeyCommand {
  val command = Commands.PFADD
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  RequireClientProtocol(elements.nonEmpty, "elements must not be empty")

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PFADD, keyBuf) ++ elements)
}

case class PFCount(keysBuf: Seq[Buf]) extends StrictKeysCommand {
  override def keys: Seq[ChannelBuffer] = keysBuf.map(BufChannelBuffer.apply)

  val command = Commands.PFCOUNT

  override def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.PFCOUNT +: keysBuf)
}

case class PFMerge(destKey: Buf, srcKeys: Seq[Buf]) extends Command {
  val command = Commands.PFMERGE

  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")

  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PFMERGE, destKey) ++ srcKeys)
}
