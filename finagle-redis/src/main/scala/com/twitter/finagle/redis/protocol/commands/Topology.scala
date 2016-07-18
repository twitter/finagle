package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class TopologyAdd(keyBuf: Buf, valueBuf: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def value: ChannelBuffer = BufChannelBuffer(valueBuf)
  def command: String = Commands.TOPOLOGYADD
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.TOPOLOGYADD, keyBuf, valueBuf))
}

case class TopologyGet(keyBuf: Buf)
  extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command: String = Commands.TOPOLOGYGET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.TOPOLOGYGET, keyBuf))
}

case class TopologyDelete(keyBuf: Buf)
  extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command: String = Commands.TOPOLOGYDELETE
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.TOPOLOGYDELETE, keyBuf))
}
