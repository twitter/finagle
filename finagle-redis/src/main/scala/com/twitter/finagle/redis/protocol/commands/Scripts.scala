package com.twitter.finagle.redis.protocol

import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Eval(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer])
  extends ScriptCommand
  with KeysCommand {
  override val command = Commands.EVAL
  val nKeys = ChannelBuffers.wrappedBuffer(keys.length.toString.getBytes(Charsets.Utf8))
  override def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EVAL, script, nKeys) ++ keys ++ argv)
  }
}

case class EvalSha(sha: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer])
  extends ScriptDigestCommand
  with KeysCommand {
  override val command = Commands.EVALSHA
  val nKeys = ChannelBuffers.wrappedBuffer(keys.length.toString.getBytes(Charsets.Utf8))
  override def toChannelBuffer = {
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.EVALSHA, sha, nKeys) ++ keys ++ argv)
  }
}

case class ScriptExists(digests: Seq[ChannelBuffer]) extends Command {
  override val command = Commands.SCRIPTEXISTS
  override def toChannelBuffer = {
    // "SCRIPT EXISTS" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SCRIPT, CommandBytes.EXISTS) ++ digests)
  }
}

object ScriptFlush extends Command {
  override val command = Commands.SCRIPTFLUSH
  override def toChannelBuffer = {
    // "SCRIPT FLUSH" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SCRIPT, CommandBytes.FLUSH))
  }
}

case class ScriptLoad(script: ChannelBuffer) extends ScriptCommand {
  override val command = Commands.SCRIPTLOAD
  override def toChannelBuffer = {
    // "SCRIPT LOAD" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedFormat(Seq(CommandBytes.SCRIPT, CommandBytes.LOAD, script))
  }
}