package com.twitter.finagle.memcachedx.protocol.text.server

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.finagle.memcachedx.protocol.ClientError
import com.twitter.finagle.memcachedx.protocol.text._
import com.twitter.finagle.memcachedx.util.ChannelBufferUtils._
import com.twitter.finagle.memcachedx.util.ParserUtils
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.util.StateMachine

class Decoder(storageCommands: collection.Set[ChannelBuffer]) extends AbstractDecoder with StateMachine {

  case class AwaitingCommand() extends State
  case class AwaitingData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) extends State

  final protected[memcachedx] def start() {
    state = AwaitingCommand()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    super.exceptionCaught(ctx, e)
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Decoding = {
    state match {
      case AwaitingCommand() =>
        decodeLine(buffer, needsData(_)) { tokens =>
          Tokens(tokens.map { ChannelBufferBuf(_) })
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          TokensWithData(tokens.map { ChannelBufferBuf(_) }, ChannelBufferBuf(data))
        }
    }
  }

  final protected[memcachedx] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int) = {
    state = AwaitingData(tokens, bytesNeeded)
    needMoreData
  }

  private[this] def needsData(tokens: Seq[ChannelBuffer]) = {
    val commandName = tokens.head
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(tokens)
      val bytesNeeded = tokens(4).toInt
      Some(bytesNeeded)
    } else None
  }

  private[this] val needMoreData = null

  private[this] def validateStorageCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 5) throw new ClientError("Too few arguments")
    if (tokens.size > 6) throw new ClientError("Too many arguments")
    if (!ParserUtils.isDigits(tokens(4))) throw new ClientError("Bad frame length")
  }
}
