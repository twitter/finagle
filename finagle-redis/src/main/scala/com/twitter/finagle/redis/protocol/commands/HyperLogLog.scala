package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class PFAdd(key: Buf, elements: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(elements.nonEmpty, "elements must not be empty")

  def name: Buf = Command.PFADD
  override def body: Seq[Buf] = key +: elements
}

case class PFCount(keys: Seq[Buf]) extends StrictKeysCommand {
  def name: Buf = Command.PFCOUNT
}

case class PFMerge(destKey: Buf, srcKeys: Seq[Buf]) extends Command {
  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")

  def name: Buf = Command.PFMERGE
  override def body: Seq[Buf] = destKey +: srcKeys
}
