package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands.trimList
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Eval(
    script: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer],
    args: Option[CommandArgument] = None)
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
    keys: Seq[String],
    args: Option[CommandArgument]) =
      new Eval(
        StringToChannelBuffer(script),
        numkeys,
        keys.map(StringToChannelBuffer(_)),
        args)
}

case class EvalSha(
    sha1: ChannelBuffer,
    numkeys: Int,
    keys: Seq[ChannelBuffer],
    args: Option[CommandArgument] = None)
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
    keys: Seq[String],
    args: Option[CommandArgument]) =
      new EvalSha(
        StringToChannelBuffer(sha1),
        numkeys,
        keys.map(StringToChannelBuffer(_)),
        args)

}

/**
 * Helper Traits
 */

abstract class Scripts extends KeysCommand {
  val scriptSha1: ChannelBuffer
  val numkeys: Int
  val keys: Seq[ChannelBuffer]
  val args: Option[CommandArgument]
  def commandBytes: ChannelBuffer

  override protected def validate() {
    super.validate()
    RequireClientProtocol(scriptSha1.readableBytes > 0, "script/sha1 must not be empty")
    RequireClientProtocol(numkeys > 0, "numkeys must be > 0")
    RequireClientProtocol(keys.size == numkeys, "must supply the same number of keys as numkeys")
  }

  def toChannelBuffer = {
    var command = Seq(scriptSha1, StringToChannelBuffer(numkeys.toString))
    val arguments: Seq[ChannelBuffer] = args match {
      case Some(WithArgs) => Seq(WithArgs.toChannelBuffer)
      case None => Nil
    }
    RedisCodec.toUnifiedFormat(commandBytes +: (command ++ keys ++ arguments))
  }
}

trait ScriptsCompanion {
  def apply(scriptSha1: String, keys: Seq[String]) = get(scriptSha1, keys.length, keys, None)
  def apply(scriptSha1: String, keys: Seq[String], args: Option[CommandArgument]) = get(scriptSha1, keys.length, keys, args)

  def get(s: String, n: Int, k: Seq[String], a: Option[CommandArgument]): Scripts

  def apply(args: Seq[Array[Byte]]) = BytesToString.fromList(args) match {
    case script :: nk :: tail =>
      val numkeys = RequireClientProtocol.safe { NumberFormat.toInt(nk) }
      tail.size match {
        case _ =>
          throw ClientError("Specified keys must equal numkeys")
      }
    case _ => throw ClientError("Expected a minimum of 3 arguments for command")
  }
}
