package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class TopologyAdd(key: Buf, value: Buf)
  extends StrictKeyCommand
  with StrictValueCommand {

  def command: String = Commands.TOPOLOGYADD
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.TOPOLOGYADD, key, value))
}

case class TopologyGet(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.TOPOLOGYGET
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.TOPOLOGYGET, key))
}

case class TopologyDelete(key: Buf) extends StrictKeyCommand {
  def command: String = Commands.TOPOLOGYDELETE
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(
    CommandBytes.TOPOLOGYDELETE, key))
}
