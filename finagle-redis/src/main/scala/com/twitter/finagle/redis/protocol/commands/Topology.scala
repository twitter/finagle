package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.protocol.Commands.trimList
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

object TopologyAdd {
  def apply(args: Seq[Array[Byte]]): TopologyAdd = {
    val list = trimList(args, 2, "TOPOLOGYADD")
    new TopologyAdd(
      Buf.ByteArray.Owned(list(0)),
      Buf.ByteArray.Owned(list(1)))
  }
}

case class TopologyGet(keyBuf: Buf)
  extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command: String = Commands.TOPOLOGYGET
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.TOPOLOGYGET, keyBuf))
}

object TopologyGet {
  def apply(args: Seq[Array[Byte]]): TopologyGet = {
    val list = trimList(args, 1, "TOPOLOGYGET")
    new TopologyGet(
      Buf.ByteArray.Owned(list(0)))
  }
}

case class TopologyDelete(keyBuf: Buf)
  extends StrictKeyCommand {
  def key: ChannelBuffer = BufChannelBuffer(keyBuf)
  def command: String = Commands.TOPOLOGYDELETE
  def toChannelBuffer: ChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(
    CommandBytes.TOPOLOGYDELETE, keyBuf))
}

object TopologyDelete {
  def apply(args: Seq[Array[Byte]]): TopologyDelete = {
    val list = trimList(args, 1, "TOPOLOGYDELETE")
    new TopologyDelete(
      Buf.ByteArray.Owned(list(0)))
  }
}


