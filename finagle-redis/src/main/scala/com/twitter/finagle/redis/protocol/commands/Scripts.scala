package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.io.{Charsets, Buf}
import org.jboss.netty.buffer.ChannelBuffer

case class Eval(script: Buf, bufKeys: Seq[Buf], argv: Seq[Buf])
  extends ScriptCommand
  with KeysCommand {
  override def command: String = Commands.EVAL
  val keys: Seq[ChannelBuffer] = bufKeys.map(ChannelBufferBuf.Owned.extract _)
  val nKeys: Buf = Buf.ByteArray.Owned(keys.length.toString.getBytes(Charsets.Utf8))
  override def toChannelBuffer: ChannelBuffer = {
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.EVAL, script, nKeys) ++ bufKeys ++ argv)
  }
}

case class EvalSha(sha: Buf, bufKeys: Seq[Buf], argv: Seq[Buf])
  extends ScriptDigestCommand
  with KeysCommand {
  override def command: String = Commands.EVALSHA
  val keys: Seq[ChannelBuffer] = bufKeys.map(ChannelBufferBuf.Owned.extract _)
  val nKeys: Buf = Buf.ByteArray.Owned(keys.length.toString.getBytes(Charsets.Utf8))
  override def toChannelBuffer: ChannelBuffer = {
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.EVALSHA, sha, nKeys) ++ bufKeys ++ argv)
  }
}

case class ScriptExists(digests: Seq[Buf]) extends Command {
  override def command: String = Commands.SCRIPTEXISTS
  override def toChannelBuffer: ChannelBuffer = {
    // "SCRIPT EXISTS" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SCRIPT, CommandBytes.EXISTS) ++ digests)
  }
}

object ScriptFlush extends Command {
  override def command: String = Commands.SCRIPTFLUSH
  override def toChannelBuffer: ChannelBuffer = {
    // "SCRIPT FLUSH" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SCRIPT, CommandBytes.FLUSH))
  }
}

case class ScriptLoad(script: Buf) extends ScriptCommand {
  override def command: String = Commands.SCRIPTLOAD
  override def toChannelBuffer: ChannelBuffer = {
    // "SCRIPT LOAD" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.SCRIPT, CommandBytes.LOAD, script))
  }
}
