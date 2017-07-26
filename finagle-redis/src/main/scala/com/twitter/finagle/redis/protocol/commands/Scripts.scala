package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class Eval(script: Buf, keys: Seq[Buf], argv: Seq[Buf])
    extends ScriptCommand
    with KeysCommand {

  def name: Buf = Command.EVAL
  override def body: Seq[Buf] = Seq(script, Buf.Utf8(keys.length.toString)) ++ keys ++ argv
}

case class EvalSha(sha: Buf, keys: Seq[Buf], argv: Seq[Buf])
    extends ScriptDigestCommand
    with KeysCommand {

  def name: Buf = Command.EVALSHA
  override def body: Seq[Buf] = Seq(sha, Buf.Utf8(keys.length.toString)) ++ keys ++ argv
}

case class ScriptExists(digests: Seq[Buf]) extends Command {
  def name: Buf = Command.SCRIPT
  override def body: Seq[Buf] =
    // "SCRIPT EXISTS" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    Buf.Utf8("EXISTS") +: digests
}

object ScriptFlush extends Command {
  def name: Buf = Command.SCRIPT
  override def body: Seq[Buf] =
    // "SCRIPT FLUSH" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    Seq(Buf.Utf8("FLUSH"))

}

case class ScriptLoad(script: Buf) extends ScriptCommand {
  def name: Buf = Command.SCRIPT
  override def body: Seq[Buf] =
    // "SCRIPT LOAD" is actually a subcommand, so we have to send "SCRIPT" and "EXISTS" separately
    Seq(Buf.Utf8("LOAD"), script)
}
