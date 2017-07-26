package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class TopologyAdd(key: Buf, value: Buf) extends StrictKeyCommand with StrictValueCommand {

  def name: Buf = Command.TOPOLOGYADD
  override def body: Seq[Buf] = Seq(key, value)
}

case class TopologyGet(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.TOPOLOGYGET
}

case class TopologyDelete(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.TOPOLOGYDELETE
}
