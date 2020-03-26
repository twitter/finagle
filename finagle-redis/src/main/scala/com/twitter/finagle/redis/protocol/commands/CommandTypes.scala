package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

trait KeyCommand extends Command {
  def key: Buf
  protected def validate(): Unit = {
    RequireClientProtocol(key != null && key.length > 0, "Empty Key found")
  }

  override def body: Seq[Buf] = Seq(key)
}
trait StrictKeyCommand extends KeyCommand {
  validate()
}

trait KeysCommand extends Command {
  def keys: Seq[Buf]
  protected def validate(): Unit = {
    RequireClientProtocol(keys != null && keys.nonEmpty, "Empty KeySet found")
    keys.foreach { key => RequireClientProtocol(key != null && key.length > 0, "Empty key found") }
  }

  override def body: Seq[Buf] = keys
}
trait StrictKeysCommand extends KeysCommand {
  validate()
}

trait ValueCommand extends Command {
  def value: Buf
}
trait StrictValueCommand extends ValueCommand {
  RequireClientProtocol(value != null && value.length > 0, "Found unexpected empty value")
}

trait MemberCommand extends Command {
  def member: Buf
}
trait StrictMemberCommand extends MemberCommand {
  RequireClientProtocol(member != null && member.length > 0, "Found unexpected empty set member")
}

trait MoveCommand extends Command {
  def source: Buf
  def destination: Buf

  override def body: Seq[Buf] = Seq(source, destination)
}

// Command that takes a script as a parameter, i.e. EVAL, SCRIPT LOAD
trait ScriptCommand extends Command {
  val script: Buf
}

// Command that takes a SHA1 digest of a script as a parameter, i.e. EVALSHA, SCRIPT EXISTS
trait ScriptDigestCommand extends Command {
  val sha: Buf
}
