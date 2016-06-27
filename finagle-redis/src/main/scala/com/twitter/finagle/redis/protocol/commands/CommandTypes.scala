package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

trait KeyCommand extends Command {
  // TODO: remove this method after netty3
  def key: ChannelBuffer
  protected def validate() {
    RequireClientProtocol(key != null && key.readableBytes > 0, "Empty Key found")
  }
}
trait StrictKeyCommand extends KeyCommand {
  validate()
}

trait KeysCommand extends Command {
  // TODO: remove this method after netty3
  def keys: Seq[ChannelBuffer]
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
  def value: ChannelBuffer
}
trait StrictValueCommand extends ValueCommand {
  RequireClientProtocol(value != null && value.readableBytes > 0,
    "Found unexpected empty value")
}

trait MemberCommand extends Command {
  def member: ChannelBuffer
}
trait StrictMemberCommand extends MemberCommand {
  RequireClientProtocol(member != null && member.readableBytes > 0,
    "Found unexpected empty set member")
}

// Command that takes a script as a parameter, i.e. EVAL, SCRIPT LOAD
trait ScriptCommand extends Command {
  val script: Buf
}

// Command that takes a SHA1 digest of a script as a parameter, i.e. EVALSHA, SCRIPT EXISTS
trait ScriptDigestCommand extends Command {
  val sha: Buf
}
