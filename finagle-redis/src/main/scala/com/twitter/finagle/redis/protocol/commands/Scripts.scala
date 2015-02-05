package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Eval(
    scriptSha1: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer])
  extends Scripts
{
  def command = Commands.EVAL
  def commandBytes = CommandBytes.EVAL
  validate()
}

object Eval extends ScriptsCompanion {
  def get(
    script: String,
    numkeys: Int,
    keys: Seq[String]) =
      new Eval(
        StringToChannelBuffer(script),
        numkeys,
        keys.map(StringToChannelBuffer(_)))
}

case class EvalSha(
    scriptSha1: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer])
  extends Scripts
{
  def command = Commands.EVALSHA
  def commandBytes = CommandBytes.EVALSHA
  validate()
}

object EvalSha extends ScriptsCompanion {
  def get(
    sha1: String,
    numkeys: Int,
    keys: Seq[String]) =
      new EvalSha(
        StringToChannelBuffer(sha1),
        numkeys,
        keys.map(StringToChannelBuffer(_)))
}

/**
 * Helper Traits
 */

abstract class Scripts extends KeysCommand {
  val scriptSha1: ChannelBuffer
  val numkeys: Int
  val keys: Seq[ChannelBuffer]
  def commandBytes: ChannelBuffer

  override protected def validate() {
    super.validate()
    RequireClientProtocol(scriptSha1.readableBytes > 0, "script/sha1 must not be empty")
    RequireClientProtocol(numkeys > 0, "numkeys must be > 0")
    RequireClientProtocol(keys.size >= numkeys, "must supply the same number of keys as numkeys")
  }

  def toChannelBuffer = {
    var command = Seq(scriptSha1, StringToChannelBuffer(numkeys.toString))
    RedisCodec.toUnifiedFormat(commandBytes +: (command ++ keys))
  }
}

trait ScriptsCompanion {
  def apply(scriptSha1: String, keys: Seq[String]) = get(scriptSha1, keys.length, keys)

  def get(s: String, n: Int, k: Seq[String]): Scripts

  def apply(args: Seq[Array[Byte]]) = BytesToString.fromList(args) match {
    case scriptSha1 :: nk :: tail =>
      val numkeys = RequireClientProtocol.safe { NumberFormat.toInt(nk) }
      tail.size match {
        case done if done >= numkeys =>
          get(scriptSha1, numkeys, tail)
        case _ =>
          throw ClientError("Specified keys must equal numkeys")
      }
    case _ => throw ClientError("Expected a minimum of 3 arguments for command")
  }
}
