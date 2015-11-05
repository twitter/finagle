package com.twitter.finagle.redis.protocol

import org.jboss.netty.buffer.ChannelBuffer

trait KeyCommand extends Command {
  val key: ChannelBuffer
  protected def validate() {
    RequireClientProtocol(key != null && key.readableBytes > 0, "Empty Key found")
  }
}
trait StrictKeyCommand extends KeyCommand {
  validate()
}

trait KeysCommand extends Command {
  val keys: Seq[ChannelBuffer]
  protected def validate() {
    RequireClientProtocol(keys != null && !keys.isEmpty, "Empty KeySet found")
    keys.foreach { key =>
      RequireClientProtocol(key != null && key.readableBytes > 0, "Empty key found")
    }
  }
}
trait StrictKeysCommand extends KeysCommand {
  validate()
}

trait ValueCommand extends Command {
  val value: ChannelBuffer
}
trait StrictValueCommand extends ValueCommand {
  RequireClientProtocol(value != null && value.readableBytes > 0,
    "Found unexpected empty value")
}

trait MemberCommand extends Command {
  val member: ChannelBuffer
}
trait StrictMemberCommand extends MemberCommand {
  RequireClientProtocol(member != null && member.readableBytes > 0,
    "Found unexpected empty set member")
}

// Command that takes a script as a parameter, i.e. EVAL, SCRIPT LOAD
trait ScriptCommand extends Command {
  val script: ChannelBuffer
}

// Command that takes a SHA1 digest of a script as a parameter, i.e. EVALSHA, SCRIPT EXISTS
trait ScriptDigestCommand extends Command {
  val sha: ChannelBuffer
}