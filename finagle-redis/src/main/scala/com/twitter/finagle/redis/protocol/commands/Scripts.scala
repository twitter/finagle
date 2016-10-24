package com.twitter.finagle.redis.protocol

import com.twitter.io.{Charsets, Buf}

case class Eval(script: Buf, keys: Seq[Buf], argv: Seq[Buf])
  extends ScriptCommand
  with KeysCommand {

  def command: String = Commands.EVAL
  def toBuf: Buf = {
    val nKeys = Buf.Utf8(keys.length.toString)
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.EVAL, script, nKeys) ++ keys ++ argv)
  }
}

case class EvalSha(sha: Buf, keys: Seq[Buf], argv: Seq[Buf])
  extends ScriptDigestCommand
  with KeysCommand {

  def command: String = Commands.EVALSHA
  def toBuf: Buf = {
    val nKeys = Buf.Utf8(keys.length.toString)
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.EVALSHA, sha, nKeys) ++ keys ++ argv)
  }
}

case class ScriptExists(digests: Seq[Buf]) extends Command {
  def command: String = Commands.SCRIPTEXISTS
  def toBuf: Buf =
    // "SCRIPT EXISTS" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.SCRIPT, CommandBytes.EXISTS) ++ digests)
}

object ScriptFlush extends Command {
  def command: String = Commands.SCRIPTFLUSH
  def toBuf: Buf =
    // "SCRIPT FLUSH" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.SCRIPT, CommandBytes.FLUSH))
}

case class ScriptLoad(script: Buf) extends ScriptCommand {
  def command: String = Commands.SCRIPTLOAD
  def toBuf: Buf =
    // "SCRIPT LOAD" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    RedisCodec.toUnifiedBuf(Seq(CommandBytes.SCRIPT, CommandBytes.LOAD, script))
}
