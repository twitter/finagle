package com.twitter.finagle.memcached.protocol.text.server

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils

class Decoder(storageCommands: collection.Set[ChannelBuffer]) extends AbstractDecoder with StateMachine {
  import ParserUtils._

  case class AwaitingCommand() extends State
  case class AwaitingData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) extends State

  final protected[memcached] def start() {
    state = AwaitingCommand()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    super.exceptionCaught(ctx, e)
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Decoding = {
    state match {
      case AwaitingCommand() =>
        decodeLine(buffer, needsData(_)) { tokens =>
          Tokens(tokens)
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          TokensWithData(tokens, data)
        }
    }
  }

  final protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) = {
    state = AwaitingData(tokens, bytesNeeded)
    needMoreData
  }

  private[this] def needsData(tokens: Seq[ChannelBuffer]) = {
    val commandName = tokens.head
    val args = tokens.tail
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(args)
      val bytesNeeded = tokens(4).toInt
      Some(bytesNeeded)
    } else None
  }

  private[this] val needMoreData = null

  private[this] def validateStorageCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 4) throw new ClientError("Too few arguments")
    if (tokens.size > 5) throw new ClientError("Too many arguments")
    if (!tokens(3).matches(DIGITS)) throw new ClientError("Bad frame length")
  }
}
