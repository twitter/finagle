package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

class Decoder(storageCommands: collection.Set[ChannelBuffer]) extends AbstractDecoder with StateMachine {

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
        decodeLine(buffer, needsData) { tokens =>
          Tokens(tokens.map { ChannelBufferBuf.Owned(_) })
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          TokensWithData(tokens.map { ChannelBufferBuf.Owned(_) }, ChannelBufferBuf.Owned(data))
        }
    }
  }

  final protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) {
    state = AwaitingData(tokens, bytesNeeded)
  }

  private[this] def needsData(tokens: Seq[ChannelBuffer]): Int = {
    val commandName = tokens.head
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(tokens)
      val bytesNeeded = tokens(4).toInt
      bytesNeeded
    } else -1
  }

  private[this] def validateStorageCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 5) throw new ClientError("Too few arguments")
    if (tokens.size > 6) throw new ClientError("Too many arguments")
    if (!ParserUtils.isDigits(tokens(4))) throw new ClientError("Bad frame length")
  }
}
