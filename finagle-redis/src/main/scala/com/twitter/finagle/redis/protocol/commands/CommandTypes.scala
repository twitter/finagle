package com.twitter.finagle.redis
package protocol

trait KeyCommand extends Command {
  val key: String
  protected def validate() {
    RequireClientProtocol(key != null && key.length > 0, "Empty Key found")
  }
}
trait StrictKeyCommand extends KeyCommand {
  validate()
}

trait ByteKeyCommand extends Command {
  val key: Array[Byte]
  protected def validate() {
    RequireClientProtocol(key != null && key.length > 0, "Empty Key found")
  }
}
trait StrictByteKeyCommand extends ByteKeyCommand {
  validate()
}

trait ByteKeysCommand extends Command {
  val keys: List[Array[Byte]]
  protected def validate() {
    RequireClientProtocol(keys.nonEmpty, "Empty key list found")
  }
}
trait StrictByteKeysCommand extends ByteKeyCommand {
  validate()
}

trait KeysCommand extends Command {
  val keys: List[String]
  protected def validate() {
    RequireClientProtocol(keys != null && keys.length > 0, "Empty KeySet found")
    keys.foreach { key => RequireClientProtocol(key != null && key.length > 0, "Empty key found") }
  }
}
trait StrictKeysCommand extends KeysCommand {
  validate()
}

trait ValueCommand extends Command {
  val value: Array[Byte]
}
trait StrictValueCommand extends ValueCommand {
  RequireClientProtocol(value != null && value.length > 0, "Found unexpected empty value")
}

trait MemberCommand extends Command {
  val member: Array[Byte]
}
trait StrictMemberCommand extends MemberCommand {
  RequireClientProtocol(member != null && member.length > 0, "Found unexpected empty set member")
}
