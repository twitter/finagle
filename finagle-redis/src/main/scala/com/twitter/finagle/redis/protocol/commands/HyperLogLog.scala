package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class PFAdd(key: Buf, elements: Seq[Buf]) extends StrictKeyCommand {
  def command: String = Commands.PFADD
  RequireClientProtocol(elements.nonEmpty, "elements must not be empty")
  def toBuf: Buf = RedisCodec.toUnifiedBuf(Seq(CommandBytes.PFADD, key) ++ elements)
}

case class PFCount(keys: Seq[Buf]) extends StrictKeysCommand {
  def command: String = Commands.PFCOUNT
  def toBuf: Buf = RedisCodec.toUnifiedBuf(CommandBytes.PFCOUNT +: keys)
}

case class PFMerge(destKey: Buf, srcKeys: Seq[Buf]) extends Command {
  def command: String = Commands.PFMERGE
  RequireClientProtocol(srcKeys.nonEmpty, "srcKeys must not be empty")
  def toBuf: Buf =
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.PFMERGE, destKey) ++ srcKeys)
}
